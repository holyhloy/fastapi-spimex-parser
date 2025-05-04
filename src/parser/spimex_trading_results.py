import asyncio
import datetime
import re
import os

import xlrd

import aiofiles
import aiohttp
from dns.dnssec import validate
from sqlalchemy import select
from sqlalchemy.sql.expression import func

import pandas as pd

from src.database import Session
from src.models.spimex_trading_results import SpimexTradingResult

if not os.path.isdir('src/parser/tables/'):
    os.makedirs('src/parser/tables/', exist_ok=True)  # pragma: no cover


class URLManager:

    def __init__(self) -> None:
        self.url = 'https://spimex.com/markets/oil_products/trades/results/'
        self.page_number = 0
        self.href_pattern = re.compile(r'/upload/reports/oil_xls/oil_xls_202[3-9]\d*')
        self.tables_hrefs = []
        self.existing_files = os.listdir('src/parser/tables/')
        self.dataframes = {}
        self.instances = []

    async def get_data_from_query(self) -> None | bool:
        print('Getting data from URL...')
        async with aiohttp.ClientSession() as session:
            fetched_hrefs = 0
            all_downloaded = False
            relevance = False
            while not all_downloaded:
                if relevance:
                    break
                self.page_number += 1
                async with session.get(self.url + f'?page=page-{self.page_number}') as response:
                    data = await response.text()
                hrefs = re.findall(self.href_pattern, data)

                if hrefs:
                    newest_href = hrefs[0]

                    newest_date = '{0}.{1}.{2}'.format(newest_href[-8:-6],
                                                       newest_href[-10:-8],
                                                       newest_href[-14:-10])
                    newest_date = datetime.datetime.strptime(newest_date, '%d.%m.%Y').date()

                    relevance = await self._check_relevance(newest_date)

                    for href in hrefs:
                        if f'https://spimex.com{href}' not in self.tables_hrefs:
                            href = f'https://spimex.com{href}'
                            self.tables_hrefs.append(href)
                            fetched_hrefs += 1
                        else:
                            all_downloaded = True
                            print(f'{fetched_hrefs} new tables hrefs have been fetched')
                else:
                    print(f'{fetched_hrefs} new tables hrefs have been fetched')
                    break
        return relevance

    async def download_tables(self) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = []
            downloaded = 0
            if self.tables_hrefs:
                print('Downloading tables...')
                for href in self.tables_hrefs:
                    file_path = f'src/parser/tables/{href[-22:]}.xls'
                    if f'{href[-22:]}.xls' not in self.existing_files:
                        tasks.append(self._download_table_file(session, href, file_path))
                        downloaded += 1
            await asyncio.gather(*tasks)
            print(f'{downloaded} tables have been downloaded' if downloaded else 'All tables are already downloaded')

    async def _download_table_file(self, session, url, file_path) -> None:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                async with aiofiles.open(file_path, 'wb') as table_file:
                    await table_file.write(content)

    async def _check_relevance(self, last_url_date) -> bool:  # pragma: no cover
        async with Session() as session:
            stmt = await session.execute(func.max(SpimexTradingResult.date))
            last_database_date = stmt.scalar()
            return last_url_date == last_database_date

    def convert_to_df(self) -> None:
        print('Converting tables to dataframes...')
        converted = 0
        for table_file in os.listdir('src/parser/tables/'):
            file_path = f'src/parser/tables/{table_file}'
            df = pd.read_excel(file_path, usecols='B:F,O', engine='xlrd')
            self.dataframes[file_path] = df
            converted += 1
        print(f'{converted} tables have been converted to dataframes')

    def validate_tables(self) -> None:
        print('Validating tables...')
        validated = 0
        search_tonn = 'Единица измерения: Метрическая тонна'
        table_borders_pattern = re.compile(r'\b(?=[A-Z-])([A-Z0-9-]+[A-Z]+[A-Z0-9-]*)\b')
        prev_df_length = 1
        for file_path, df in self.dataframes.items():
            tonn_index = df.loc[df.isin([search_tonn]).any(axis=1)].index.tolist()
            new_df = pd.read_excel(file_path, header=tonn_index[0] + 2, usecols='B:F,O', skiprows=[tonn_index[0] + 3])
            first_column_list = new_df['Код\nИнструмента'].tolist()
            footer_index = 0
            for code in first_column_list:
                if re.match(table_borders_pattern, code):
                    continue
                footer_index = first_column_list.index(code)
                break
            new_df = new_df[:footer_index - 1]
            new_df.columns = ['exchange_product_id',
                              'exchange_product_name',
                              'delivery_basis_name',
                              'volume',
                              'total',
                              'count']
            new_df = new_df[new_df['count'] != '-']
            new_df = new_df.reset_index(drop=True)
            new_df['id'] = pd.RangeIndex(prev_df_length, len(new_df) + prev_df_length)
            prev_df_length += len(new_df)
            new_df.set_index(['id'], inplace=True, drop=True)
            self.dataframes[file_path] = new_df
            validated += 1
        print(f'{validated} dataframes have been validated')

    def add_columns(self) -> None:
        print('Adding columns...')
        extended_df = 0
        extended_rows = 0
        for path, df in self.dataframes.items():
            date = '{0}.{1}.{2}'.format(path[-12:-10], path[-14:-12], path[-18:-14])
            date = datetime.datetime.strptime(date, '%d.%m.%Y').date()
            df['date'] = date
            df['created_on'] = datetime.date.today()
            extended_df += 1
            for index, row in df.iterrows():
                oil_id = row['exchange_product_id'][:4]
                delivery_basis_id = row['exchange_product_id'][4:7]
                delivery_type_id = row['exchange_product_id'][-1]

                df.loc[index, 'oil_id'] = oil_id
                df.loc[index, 'delivery_basis_id'] = delivery_basis_id
                df.loc[index, 'delivery_type_id'] = delivery_type_id
                extended_rows += 1
        print(f'{extended_df} dataframes ({extended_rows} rows) have been extended')

    async def load_to_db(self) -> None:
        print('Loading to database...')
        df_affected = 0
        rows_affected = 0
        tasks = []

        async with Session() as session:
            existing_ids_query = await session.execute(select(SpimexTradingResult.id))
            existing_ids = set(row for row in existing_ids_query.scalars().all())
            for file_path, df in self.dataframes.items():
                df_affected += 1
                for index, row in df.iterrows():
                    if index in existing_ids:
                        df.loc[index, 'updated_on'] = datetime.date.today()
                    else:
                        df.loc[index, 'updated_on'] = None
                        df.loc[index, 'created_on'] = datetime.date.today()
                        rows_affected += 1
                        tasks.append(self._convert_decorator(row))

            await asyncio.gather(*tasks)
            session.add_all(self.instances)
            await session.commit()
            print(f'{df_affected} dataframes ({rows_affected} rows) have been inserted')

    async def _convert_decorator(self, row) -> None:
        await self._convert_row_to_model(row)

    async def _convert_row_to_model(self, row) -> None:
        row = SpimexTradingResult(**row.to_dict())
        self.instances.append(row)
