import pytest



@pytest.mark.asyncio
async def test_get_last_trading_dates(client, mocker):
    mock_last_dates = ["2025-04-22", "2025-04-23"]
    mocker.patch("src.api.service.get_last_trading_dates", return_value=mock_last_dates)

    response = await client.get("/last_dates?amount=2")

    assert response.status_code == 200
    assert response.json() == {'success': True,
                               'last_trading_dates': mock_last_dates}
