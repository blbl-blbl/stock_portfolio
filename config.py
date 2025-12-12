
# Возможные значения для столбца 'Operation' в operation_history
available_sell_operations = ['sell', 'продать','продала', 'шорт', 'short', 'продал']
available_buy_operations = ['buy', 'купить', 'купила', 'лонг', 'long','купил']

# Ссылка на API Мосбиржи для сбора данных по акциям
shares_url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities.json'
# Ссылка на API Мосбиржи для сбора данных по облигациям
bonds_url = 'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'
# Ссылка на API Мосбиржии для сбора данных по валютам
currencies_url = 'https://iss.moex.com/iss/engines/currency/markets/index/securities.json'

# Данные для парсинга с маркетдаты. Формат:
# тип актива: ['engine в маркетдате', 'market в маркетдате', 'название таблицы для sql']
urls_settings = {'currency' : ['currency', 'index', 'marketdata_currency', 1, 4, currencies_url],
                 'shares' : ['stock', 'shares', 'marketdata_shares', 0, 2, shares_url],
                 'bonds' : ['stock', 'bonds', 'marketdata_bonds', 0, 2, bonds_url]}

# Информцация о дроблении / консолидации фондового рынка
split_url = 'https://iss.moex.com/iss/statistics/engines/stock/splits.json'

# Информация по техническому изменению торговых кодов
rename_url = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/changeover.json'

