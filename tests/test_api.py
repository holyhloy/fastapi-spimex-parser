from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_get_last_trading_dates_success(client, mocker):
    mock_last_dates = ["2025-04-22", "2025-04-23"]
    mock_func = AsyncMock(return_value=mock_last_dates)
    mocker.patch("src.api.service.get_last_trading_dates", mock_func)

    response1 = await client.get("/last_dates?amount=2")

    assert response1.status_code == 200
    assert response1.json() == {'success': True,
                                'last_trading_dates': mock_last_dates}
    assert mock_func.call_count == 1

    response2 = await client.get("/last_dates?amount=2")

    assert response2.status_code == 200
    assert response2.json() == {'success': True,
                                'last_trading_dates': mock_last_dates}
    assert mock_func.call_count == 1


@pytest.mark.asyncio
async def test_get_dynamics_success(client, mocker):
    mock_dynamics = \
        [
            {'id': 1,
             'oil_id': 'A100',
             'delivery_basis_id': 'ABS',
             'delivery_type_id': 'A',
             'date': '2025-04-30'},
            {'id': 2,
             'oil_id': 'A100',
             'delivery_basis_id': 'ABS',
             'delivery_type_id': 'A',
             'date': '2025-04-25'},
            {'id': 3,
             'oil_id': 'A100',
             'delivery_basis_id': 'ABS',
             'delivery_type_id': 'A',
             'date': '2025-04-24'}
        ]

    mock_func = AsyncMock(return_value=mock_dynamics)
    mocker.patch('src.api.service.get_dynamics', mock_func)

    response1 = await client.get('/dynamics',
                                 params={
                                     'start_date': '2025-04-24',
                                     'end_date': '2025-04-30',
                                     'oil_id': 'A100',
                                     'delivery_type_id': 'A',
                                     'delivery_basis_id': 'ABS'
                                 })

    assert response1.status_code == 200
    assert response1.json() == {'success': True, 'dynamics': mock_dynamics}
    assert mock_func.call_count == 1

    # data = response1.json()

    # for row in data['dynamics']:
    #     assert 'A100' == row['oil_id']
    #     assert 'A' == row['delivery_type_id']
    #     assert 'ABS' == row['delivery_basis_id']
    #     assert (datetime.date(2025, 4, 24) <=
    #             datetime.date.fromisoformat(row['date']) <=
    #             datetime.date(2025, 4, 30))

    response2 = await client.get('/dynamics',
                                 params={
                                     'start_date': '2025-04-24',
                                     'end_date': '2025-04-30',
                                     'oil_id': 'A100',
                                     'delivery_type_id': 'A',
                                     'delivery_basis_id': 'ABS'
                                 })

    assert response2.status_code == 200
    assert response2.json() == {'success': True, 'dynamics': mock_dynamics}
    assert mock_func.call_count == 1


@pytest.mark.asyncio
async def test_get_trading_results_success(client, mocker):
    mock_last_results = \
        [
            {'id': 1,
             'oil_id': 'A100',
             'delivery_basis_id': 'ABS',
             'delivery_type_id': 'A',
             'date': '2025-04-30'},
            {'id': 2,
             'oil_id': 'A100',
             'delivery_basis_id': 'ABS',
             'delivery_type_id': 'A',
             'date': '2025-04-30'},
            {'id': 3,
             'oil_id': 'A100',
             'delivery_basis_id': 'ABS',
             'delivery_type_id': 'A',
             'date': '2025-04-30'}
        ]

    mock_func = AsyncMock(return_value=mock_last_results)
    mocker.patch('src.api.service.get_trading_results', mock_func)

    response1 = await client.get('/last_results')

    assert response1.status_code == 200
    assert response1.json() == {'success': True, 'last_trading_results': mock_last_results}
    assert mock_func.call_count == 1

    # data = response1.json()
    #
    # for row in data['last_trading_results']:
    #     assert 'A100' == row['oil_id']
    #     assert 'A' == row['delivery_type_id']
    #     assert 'ABS' == row['delivery_basis_id']

    response2 = await client.get('/last_results')

    assert response2.status_code == 200
    assert response2.json() == {'success': True, 'last_trading_results': mock_last_results}
    assert mock_func.call_count == 1
