import pandas as pd

DF_CACHE = {}


def graph_data(data):
    data.filter(['Date', 'osdh_ok_cml_cases', 'osdh_tul_cml_cases', 'nyt_ok_cml_cases',
                 'nyt_tul_cml_cases']).plot(title='Cumulative Cases', rot=90)
    data.filter(['Date', 'osdh_ok_cml_cases', 'osdh_tul_cml_cases', 'nyt_ok_cml_cases',
                 'nyt_tul_cml_cases']).plot(title='Cumulative Cases (log)', rot=90, logy=True)
    data.filter(['Date', 'osdh_ok_current_cases', 'osdh_tul_current_cases']).plot(
        title='Currently Active Cases', rot='vertical')
    data.filter(['Date', 'osdh_ok_7dr_cases', 'osdh_tul_7dr_cases', 'nyt_ok_7dr_cases',
                 'nyt_tul_7dr_cases']).plot(title='New Cases (rolling)', rot=90)


def get_combined_data(county='Tulsa'):
    osdh = pd.merge(prepare_osdh(), prepare_osdh('Tulsa', 'osdh_tul'),
                    how='outer', left_index=True, right_index=True)
    nyt = pd.merge(prepare_nyt(), prepare_nyt('Tulsa', 'nyt_tul'),
                   how='outer', left_index=True, right_index=True)
    data = pd.merge(osdh, nyt, how='outer', left_index=True, right_index=True)
    data = data.reindex(pd.date_range(data.index.min(), data.index.max()))
    return data


def prepare_osdh(county=None, prefix='osdh_ok'):
    if f'osdh-{prefix}' not in DF_CACHE:
        source = 'https://storage.googleapis.com/ok-covid-gcs-public-download/oklahoma_cases_osdh_county.csv'
        data = pd.read_csv(source)
        data['OnsetDate'] = pd.to_datetime(data['OnsetDate'])
        if county:
            data = data.query(f'County == "{county.upper()}"')
        data = data.filter(['OnsetDate', 'Active', 'Deceased', 'Recovered'])
        data = data.rename(columns={
            'OnsetDate': 'date',
            'Active': 'active_on',
            'Deceased': 'new_deaths',
            'Recovered': 'new_recovered',
        })
        data = data.groupby('date').sum()
        data['new_cases'] = data['active_on'] + data['new_deaths'] + data['new_recovered']
        data['cml_cases'] = data['new_cases'].cumsum()
        data['cml_deaths'] = data['new_deaths'].cumsum()
        data['cml_recovered'] = data['new_recovered'].cumsum()
        data['7dr_cases'] = data['new_cases'].rolling(7).mean()
        data['7dr_deaths'] = data['new_deaths'].rolling(7).mean()
        data['7dr_recovered'] = data['new_recovered'].rolling(7).mean()
        data['current_cases'] = data['cml_cases'] - data['cml_cases'].shift(14)
        DF_CACHE[f'osdh-{prefix}'] = data.rename(columns=lambda x: f'{prefix}_{x}')
    return DF_CACHE[f'osdh-{prefix}']


def prepare_nyt(county=None, prefix='nyt_ok'):
    if f'nyt-{prefix}' not in DF_CACHE:
        source = 'https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv?raw=true'
        data = pd.read_csv(source)
        data['date'] = pd.to_datetime(data['date'])
        data = data.query('state == "Oklahoma"')
        if county:
            data = data.query(f'county == "{county.title()}"')
        data = data.filter(['date', 'cases', 'deaths'])
        data = data.rename(columns={
            'cases': 'cml_cases',
            'deaths': 'cml_deaths',
        })
        data = data.groupby('date').sum()
        data['new_cases'] = data['cml_cases'].diff(periods=1)
        data['new_deaths'] = data['cml_deaths'].diff(periods=1)
        data['7dr_cases'] = data['new_cases'].rolling(7).mean()
        data['7dr_deaths'] = data['new_deaths'].rolling(7).mean()
        DF_CACHE[f'nyt-{prefix}'] = data.rename(columns=lambda x: f'{prefix}_{x}')
    return DF_CACHE[f'nyt-{prefix}']
