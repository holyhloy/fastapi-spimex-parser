import pandas as pd
import pytest
import datetime
import xlwt
from unittest.mock import AsyncMock, patch, MagicMock

from src.models.spimex_trading_results import SpimexTradingResult


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_get_data_from_query_relevance_true(mock_get, url_manager):
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="""
        <a href="/upload/reports/oil_xls/oil_xls_20250430162000.xls?r=6602"></a>
    """)

    mock_get.return_value.__aenter__.return_value = mock_response

    url_manager._check_relevance = AsyncMock(return_value=True)
    relevance = await url_manager.get_data_from_query()

    assert relevance is True
    assert isinstance(url_manager.tables_hrefs, list)


@pytest.mark.asyncio
@patch("aiofiles.open")
@patch("aiohttp.ClientSession.get")
async def test_download_tables(mock_get, mock_aiofiles_open, url_manager):
    url_manager.tables_hrefs = [
        "https://spimex.com/upload/reports/oil_xls/oil_xls_20250430162000.xls"
    ]
    url_manager.existing_files = []

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.read = AsyncMock(return_value=b"Fake content")
    mock_get.return_value.__aenter__.return_value = mock_response

    mock_file = AsyncMock()
    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__ = AsyncMock(return_value=mock_file)
    mock_context_manager.__aexit__ = AsyncMock(return_value=None)
    mock_aiofiles_open.return_value = mock_context_manager

    await url_manager.download_tables()

    mock_file.write.assert_called_once_with(b"Fake content")


@patch('os.listdir')
@patch('pandas.read_excel')
def test_convert_to_df(mock_read_excel, mock_listdir, url_manager, tmp_path):
    file_path = tmp_path / "test.xls"
    df = pd.DataFrame({
        "Код\nИнструмента": ["TEST-123"],
        "Unnamed: 1": [1],
        "Unnamed: 2": [2],
        "Unnamed: 3": [3],
        "Unnamed: 4": [4],
        "Unnamed: 5": [5],
        "Unnamed: 6": [1],
        "Unnamed: 7": [2],
        "Unnamed: 8": [3],
        "Unnamed: 9": [4],
        "Unnamed: 10": [5],
        "Unnamed: 11": [1],
        "Unnamed: 12": [2],
        "Unnamed: 13": [3],
        "Unnamed: 14": [4],
        "Unnamed: 15": [5]
    })

    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet('Sheet1')

    for row_num, row_data in df.iterrows():
        for col_num, value in enumerate(row_data):
            sheet.write(row_num, col_num, value)

    workbook.save(file_path)
    mock_listdir.return_value = ["test.xls"]
    mock_read_excel.return_value = df
    url_manager.convert_to_df()
    mock_read_excel.assert_called_once_with("src/parser/tables/test.xls", usecols="B:F,O", engine="xlrd")

    assert isinstance(url_manager.dataframes["src/parser/tables/test.xls"], pd.DataFrame)
    assert url_manager.dataframes["src/parser/tables/test.xls"].shape == (1, 16)


@patch('pandas.read_excel')
def test_validate_tables_calls_read_excel_once(mock_read_excel, url_manager):
    fake_path = 'src/parser/tables/fake_file.xls'

    base_df = pd.DataFrame({
        'B': ['other', 'Единица измерения: Метрическая тонна', '...'],
        'C': ['', '', ''],
        'D': ['', '', ''],
        'E': ['', '', ''],
        'F': ['', '', ''],
        'O': ['', '', '']
    })

    processed_df = pd.DataFrame({
        'Код\nИнструмента': ['ABCD123', 'XYZ789', 'HGTE673', 'invalid'],
        'Наименование': ['Prod 1', 'Prod 2', '', ''],
        'Базис поставки': ['Basis 1', 'Basis 2', '', ''],
        'Объем': [100, 200, '', ''],
        'Сумма': [1000, 2000, '', ''],
        'Количество': [1, 2, '', '-']
    })

    mock_read_excel.return_value = processed_df

    url_manager.dataframes = {fake_path: base_df}

    url_manager.validate_tables()

    mock_read_excel.assert_called_once_with(
        fake_path,
        header=3,  # tonn_index[0] == 1 → 1 + 2
        usecols='B:F,O',
        skiprows=[4]
    )

    result_df = url_manager.dataframes[fake_path]
    assert list(result_df.columns) == [
        'exchange_product_id',
        'exchange_product_name',
        'delivery_basis_name',
        'volume',
        'total',
        'count'
    ]
    assert result_df.index.name == 'id'
    assert len(result_df) == 2  # row with '-' excluded


def test_add_columns(url_manager):
    df = pd.DataFrame({
        "exchange_product_id": ["ABCD123X"],
        "exchange_product_name": ["Test"],
        "delivery_basis_name": ["Basis"],
        "volume": [100],
        "total": [200],
        "count": [1]
    })
    url_manager.dataframes = {"src/parser/tables/oil_xls_20250430162000.xls": df.copy()}
    url_manager.add_columns()

    updated_df = url_manager.dataframes["src/parser/tables/oil_xls_20250430162000.xls"]
    assert "oil_id" in updated_df.columns
    assert "delivery_basis_id" in updated_df.columns
    assert "delivery_type_id" in updated_df.columns


@pytest.mark.asyncio
async def test_load_to_db_inserts_new_records(mocker, url_manager):
    df = pd.DataFrame([{
        'exchange_product_id': '1234567',
        'exchange_product_name': 'Test Oil',
        'delivery_basis_name': 'Test Basis',
        'volume': 100,
        'total': 50000,
        'count': 1,
        'date': datetime.date(2024, 1, 1),
        'created_on': datetime.date.today(),
        'oil_id': '1234',
        'delivery_basis_id': '567',
        'delivery_type_id': '7',
    }])
    df.index = [10]
    url_manager.dataframes = {'dummy_path': df}

    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []

    mock_execute_result = MagicMock()
    mock_execute_result.scalars.return_value = mock_scalars

    mock_session = AsyncMock()
    mock_session.execute.return_value = mock_execute_result
    mock_session.commit = AsyncMock()
    mock_session.add_all = MagicMock()

    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__.return_value = mock_session

    mocker.patch('src.parser.spimex_trading_results.Session', return_value=mock_session_cm)

    await url_manager.load_to_db()

    assert len(url_manager.instances) == 1
    assert isinstance(url_manager.instances[0], SpimexTradingResult)
    mock_session.add_all.assert_called_once_with(url_manager.instances)
    mock_session.commit.assert_awaited()
