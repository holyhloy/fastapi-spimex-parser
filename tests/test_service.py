import datetime

import pytest

from src.api.service import parse_spimex, get_last_trading_dates, get_dynamics, get_trading_results
from src.models.spimex_trading_results import SpimexTradingResult


@pytest.fixture
def instances(session):
    instances = [
        SpimexTradingResult(id=1,
                            exchange_product_id='A592ANK060F',
                            exchange_product_name='Бензин (АИ-92-К5) по ГОСТ,'
                                                  ' Ангарск-группа станций (ст. отправления)',
                            oil_id='A600',
                            delivery_basis_id='ALI',
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
                            oil_id='A931',
                            delivery_basis_id='AVM',
                            delivery_basis_name='СН КНПЗ',
                            delivery_type_id='A',
                            volume='25',
                            total='59438602',
                            count='1',
                            date=datetime.date(2023, 1, 10),
                            created_on=datetime.date(2025, 4, 30),
                            updated_on=datetime.date(2025, 4, 30)),
        SpimexTradingResult(id=3,
                            exchange_product_id='A592ANK060F',
                            exchange_product_name='Бензин (АИ-92-К5) по ГОСТ,'
                                                  ' Ангарск-группа станций (ст. отправления)',
                            oil_id='A458',
                            delivery_basis_id='KLI',
                            delivery_basis_name='Ангарск-группа станций',
                            delivery_type_id='K',
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
async def test_parse_spimex_behavior(mocker, relevance, capfd):
    fake_parser = mocker.Mock()

    fake_parser.get_data_from_query = mocker.AsyncMock(return_value=relevance)
    fake_parser.download_tables = mocker.AsyncMock()
    fake_parser.convert_to_df = mocker.Mock()
    fake_parser.validate_tables = mocker.Mock()
    fake_parser.add_columns = mocker.Mock()
    fake_parser.load_to_db = mocker.AsyncMock()

    await parse_spimex(fake_parser)

    out, err = capfd.readouterr()

    fake_parser.get_data_from_query.assert_awaited_once()

    if relevance:
        assert 'Database has relevant data' in out
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


@pytest.mark.asyncio
async def test_get_last_trading_dates(session, instances, setup_db):
    session.add_all(instances)
    await session.commit()

    result = await get_last_trading_dates(session, amount_of_days=2)
    assert len(result) == 2
    assert result == sorted(result, reverse=True)


@pytest.mark.asyncio
async def test_get_dynamics_filters_by_period_and_fields(session, instances, setup_db):
    session.add_all(instances)
    await session.commit()

    results_only_date = await get_dynamics(session, start_date=datetime.date(2023, 1, 9),
                                 end_date=datetime.date(2023, 1, 10))
    assert len(results_only_date) == 2

    results_with_oil_id = await get_dynamics(session, start_date=datetime.date(2023, 1, 1),
                                 end_date=datetime.date(2023, 1, 10), oil_id='A458')
    assert len(results_with_oil_id) == 1

    results_with_delivery_basis_id = await get_dynamics(session, start_date=datetime.date(2023, 1, 1),
                                 end_date=datetime.date(2023, 1, 10), delivery_basis_id='AVM')
    assert len(results_with_delivery_basis_id) == 1

    results_with_delivery_type_id = await get_dynamics(session, start_date=datetime.date(2023, 1, 1),
                                                        end_date=datetime.date(2023, 1, 10), delivery_type_id='A')
    assert len(results_with_delivery_type_id) == 1


@pytest.mark.asyncio
async def test_get_trading_results_filters_by_newest_date(session, instances, setup_db):
    session.add_all(instances)
    await session.commit()

    results = await get_trading_results(session)
    assert len(results) == 2

    results_with_type_id_filter = await get_trading_results(session, delivery_type_id='A')
    assert len(results_with_type_id_filter) == 1

    results_with_oil_id_filter = await get_trading_results(session, oil_id='A600')
    assert len(results_with_oil_id_filter) == 1

    results_with_basis_id_filter = await get_trading_results(session, delivery_basis_id='KLI')
    assert len(results_with_basis_id_filter) == 0
