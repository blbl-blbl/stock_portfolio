
# Возможные значения для столбца 'Operation' в operation_history
available_sell_operations = ['sell', 'продать','продала', 'шорт', 'short', 'продал']
available_buy_operations = ['buy', 'купить', 'купила', 'лонг', 'long','купил']

# Ссылка на API Мосбиржи для сбора данных по акциям
shares_url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities.json'

# Нужные режими торгов для акций
BOARDID_SHARES = ['TQBR']
# Нужные режимы торгов для пифов
BOARDID_ETFS = ['TQTF']

# Ссылка на API Мосбиржи для сбора данных по облигациям
bonds_url = 'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'

# Нужные режимы торгов для облигаций
# Возможно нужно еще расширить (https://iss.moex.com/iss/engines/stock/markets/bonds.xml)
BOARDID_BONDS = ['TQIR', 'TQOB', 'TQOD', 'TQCB', 'TQOE', 'TQOY', 'TQRD', 'TQUD']
DEFAULT_BONDS = ['TQRD', 'TQUD']

