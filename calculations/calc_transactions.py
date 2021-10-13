import csv
from datetime import datetime

from forex_python.converter import CurrencyRates

from clearvalue.lib.currency_utils import convert_currency
from clearvalue.model.securities import SecurityHoldings, SecuritiesPortfolio
from clearvalue.model import InvestmentTransactionType, InvestmentTransaction


def load_poalim_csv():
    with open('transactions.csv', 'r') as f:
        reader = csv.reader(f)
        data = [r[:15] for r in reader]

    return data[:1], data[1:-1]


def cleanup_poalim(data):
    to_float = lambda val: float(val.replace(',', ''))
    for row in data:
        row[0] = row[0].replace('\n', ''),
        if type(row[0]) in (set, list, tuple):
            row[0] = row[0][0]
        # for indx in [4, 5, 7, 8, 10, 11, 12, 13]:
        for indx in [4, 5, 8, 9, 11, 12, 13]:
            row[indx] = to_float(row[indx])

        if row[5] > 0:
            row[5] /= 100

        if row[1] == 'קניה':
            row[1] = 'buy'
        elif row[1] == 'מכירה':
            row[1] = 'sell'
        else:
            row[1] = 'dividend'

    data.reverse()
    return data


def filter_stock(name, data):
    return [row for row in data if name in row[0]]


def fill_holdings(data, currency='USD'):
    holdings = SecurityHoldings(currency=currency)
    for row in data:
        _date = datetime.strptime(row[2].strip(' \r\n'), "%d/%m/%Y")
        if row[1] == 'buy':
            val = convert_currency(row[5], currency, 'USD', on_date=_date)
            holdings.add_transaction(InvestmentTransaction(InvestmentTransactionType.BUY, row[4], val, row[4] * val))
        elif row[1] == 'sell':
            val = convert_currency(row[5], currency, 'USD', on_date=_date)
            holdings.add_transaction(InvestmentTransaction(InvestmentTransactionType.SELL, row[4], val, row[4] * val))
        elif row[1] == 'dividend':
            holdings.add_transaction(InvestmentTransaction(InvestmentTransactionType.DIVIDEND, 0, 0, row[9]))

    return holdings


def cvkw():
    stocks = [('voo', 1133, 264.22, 283.80),
              ('hedj', 1894, 62.90, 70.37),
              ('sche', 4918, 24.23, 26.79),
              ('ICLN', 5530, 10.82, 10.59)]

    portfolio = SecuritiesPortfolio()
    for s in stocks:
        sh = SecurityHoldings(s[0], s[1], s[1] * s[2], s[2], current_share_value=s[3])
        portfolio.holdings.append(sh)
        print('>> {} total return {}'.format(sh.symbol, sh.total_return()))

    print('>>> *** total return:', portfolio.total_return())


def poalim():
    headers, data = load_poalim_csv()
    data = cleanup_poalim(data)
    vt_holdings = fill_holdings(filter_stock('VANGUARD TOT WORLD STK INDEX', data))
    vt_holdings.current_share_value = 83.08

    cr = CurrencyRates()
    rate = cr.get_rate('ILS', 'USD')

    ktf_holdings = fill_holdings(filter_stock('ktf', data), currency='ILS')
    ktf_holdings.current_share_value = 1.1575 * rate

    ibi_holdings = fill_holdings(filter_stock('מחקה', data), currency='ILS')
    ibi_holdings.current_share_value = 1.3682 * rate

    print('>>> vt_holdings total return:', vt_holdings.total_return())
    print('>>> ktf_holdings total return:', ktf_holdings.total_return())
    print('>>> ibi_holdings total return:', ibi_holdings.total_return())

    portfolio = SecuritiesPortfolio(holdings=[vt_holdings, ktf_holdings, ibi_holdings])
    print('>>> *** total return:', portfolio.total_return())
    print('>>> *** total gain:', portfolio.total_gain())
    print('>>> *** total value:', portfolio.total_value())


if __name__ == '__main__':
    # cvkw()
    poalim()
