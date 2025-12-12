
# Возможные значения для столбца 'Operation' в operation_history
available_sell_operations = ['sell', 'продать','продала', 'шорт', 'short', 'продал']
available_buy_operations = ['buy', 'купить', 'купила', 'лонг', 'long','купил']



# Нужные режими торгов для акций
BOARDID_SHARES = ['TQBR']
# Нужные режимы торгов для пифов
BOARDID_ETFS = ['TQTF']

# Возможно нужно еще расширить (https://iss.moex.com/iss/engines/stock/markets/bonds.xml)
BOARDID_BONDS = ['TQIR', 'TQOB', 'TQOD', 'TQCB', 'TQOE', 'TQOY', 'TQRD', 'TQUD']
DEFAULT_BONDS = ['TQRD', 'TQUD']

# Ссылка на API Мосбиржи для сбора данных по акциям
shares_url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities.json'
# Ссылка на API Мосбиржи для сбора данных по облигациям
bonds_url = 'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'
# Ссылка на API Мосбиржии для сбора данных по валютам
currencies_url = 'https://iss.moex.com/iss/engines/currency/markets/index/securities.json'

# Данные для парсинга с маркетдаты. Формат:
# тип актива: ['engine в маркетдате', 'market в маркетдате', 'название таблицы для sql']
urls_settings = {'currency' : ['currency', 'index', 'marketdata_currency', 1, 4, currencies_url],
                 'shares' : ['stock', 'shares', 'marketdataSares', 0, 2, shares_url],
                 'bonds' : ['stock', 'bonds', 'marketdataBonds', 0, 2, bonds_url]}

