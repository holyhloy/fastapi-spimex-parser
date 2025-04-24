### FastAPI микросервисное приложение для просмотра результатов торгов с биржи spimex

#### Реализованы функции:
- get_last_trading_dates – список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
- get_dynamics – список торгов за заданный период (фильтрация по oil_id, delivery_type_id, delivery_basis_id, start_date, end_date).
- get_trading_results – список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)

#### Реализовано кэширование запросов с хранением до 14:11 каждого дня

#### Приложение упаковано в Docker контейнер, так же как и Postgres с Redis