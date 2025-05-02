import datetime

import pytest

from src.api.service import parse_spimex
from src.models.spimex_trading_results import SpimexTradingResult


@pytest.fixture
def instances():
    instances = [
        SpimexTradingResult(id=1,
                            exchange_product_id='A592ANK060F',
                            exchange_product_name='Бензин (АИ-92-К5) по ГОСТ,'
                                                  ' Ангарск-группа станций (ст. отправления)',
                            oil_id='A592',
                            delivery_basis_id='ANK',
                            delivery_basis_name='Ангарск-группа станций',
                            delivery_type_id='F',
                            volume='600',
                            total='24775140',
                            count='10',
                            date=datetime.date(2023, 1, 10),
                            created_on=datetime.date(2025, 4, 30),
                            updated_on=datetime.date(2025, 4, 30)),
        SpimexTradingResult(id=2,
                            exchange_product_id='A592AVM005A',
                            exchange_product_name='Бензин (АИ-92-К5) по ГОСТ,'
                                                  ' СН КНПЗ (самовывоз автотранспортом)',
                            oil_id='A592',
                            delivery_basis_id='AVM',
                            delivery_basis_name='СН КНПЗ',
                            delivery_type_id='A',
                            volume='25',
                            total='59438602',
                            count='1',
                            date=datetime.date(2023, 1, 9),
                            created_on=datetime.date(2025, 4, 30),
                            updated_on=datetime.date(2025, 4, 30)),
        SpimexTradingResult(id=3,
                            exchange_product_id='A592ANK060F',
                            exchange_product_name='Бензин (АИ-92-К5) по ГОСТ,'
                                                  ' Ангарск-группа станций (ст. отправления)',
                            oil_id='A592',
                            delivery_basis_id='ANK',
                            delivery_basis_name='Ангарск-группа станций',
                            delivery_type_id='F',
                            volume='600',
                            total='24775140',
                            count='10',
                            date=datetime.date(2023, 1, 1),
                            created_on=datetime.date(2025, 4, 30),
                            updated_on=datetime.date(2025, 4, 30))
    ]
    return instances


@pytest.mark.asyncio
@pytest.mark.parametrize("relevance", [True, False])
async def test_parse_spimex_behavior(mocker, relevance):
    fake_parser = mocker.Mock()

    fake_parser.get_data_from_query = mocker.AsyncMock(return_value=relevance)
    fake_parser.download_tables = mocker.AsyncMock()
    fake_parser.convert_to_df = mocker.Mock()
    fake_parser.validate_tables = mocker.Mock()
    fake_parser.add_columns = mocker.Mock()
    fake_parser.load_to_db = mocker.AsyncMock()

    await parse_spimex(fake_parser)

    fake_parser.get_data_from_query.assert_awaited_once()

    if relevance:
        fake_parser.download_tables.assert_not_called()
        fake_parser.convert_to_df.assert_not_called()
        fake_parser.validate_tables.assert_not_called()
        fake_parser.add_columns.assert_not_called()
        fake_parser.load_to_db.assert_not_called()
    else:
        fake_parser.download_tables.assert_awaited_once()
        fake_parser.convert_to_df.assert_called_once()
        fake_parser.validate_tables.assert_called_once()
        fake_parser.add_columns.assert_called_once()
        fake_parser.load_to_db.assert_awaited_once()