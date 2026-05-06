import os
from datetime import date, timedelta

import dash
from dash import dcc, html, Input, Output

from data import load_parquet_last_24h, load_report
from pages import overview, report as report_page

REFRESH_MS = 300_000

app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Ferme intelligente"

app.layout = html.Div([
    dcc.Interval(id='interval', interval=REFRESH_MS, n_intervals=0),
    dcc.Interval(id='countdown-tick', interval=1000, n_intervals=0),
    dcc.Store(id='store-sensor'),
    dcc.Store(id='store-report-today'),
    dcc.Store(id='store-report-yesterday'),
    dcc.Store(id='store-next-refresh', data=REFRESH_MS // 1000),

    html.Nav([
        html.Span('🌾 Ferme intelligente', className='nav-title'),
        dcc.Tabs(id='tabs', value='overview', children=[
            dcc.Tab(label='Tableau de bord', value='overview'),
            dcc.Tab(label='Rapport journalier', value='report'),
        ], className='nav-tabs'),
        html.Span(id='countdown-display', className='nav-countdown'),
    ], className='navbar'),

    html.Div(id='page-content'),
])

overview.register_callbacks(app)
report_page.register_callbacks(app)


@app.callback(
    Output('store-sensor', 'data'),
    Output('store-report-today', 'data'),
    Output('store-report-yesterday', 'data'),
    Output('store-next-refresh', 'data'),
    Input('interval', 'n_intervals'),
)
def refresh_data(_n):
    df = load_parquet_last_24h()
    today_str = str(date.today())
    yesterday_str = str(date.today() - timedelta(days=1))
    today_report = load_report(today_str)
    yesterday_report = load_report(yesterday_str)
    sensor_data = df.to_dict('records') if not df.empty else []
    return sensor_data, today_report, yesterday_report, REFRESH_MS // 1000


@app.callback(
    Output('store-next-refresh', 'data', allow_duplicate=True),
    Output('countdown-display', 'children'),
    Input('countdown-tick', 'n_intervals'),
    Input('store-next-refresh', 'data'),
    prevent_initial_call=True,
)
def tick_countdown(_n, remaining):
    if remaining is None:
        remaining = REFRESH_MS // 1000
    remaining = max(0, remaining - 1)
    minutes, seconds = divmod(remaining, 60)
    label = f"🔄 Mise à jour dans {minutes}:{seconds:02d}"
    return remaining, label


@app.callback(
    Output('page-content', 'children'),
    Input('tabs', 'value'),
)
def render_page(tab):
    if tab == 'report':
        return report_page.layout()
    return overview.layout()


if __name__ == '__main__':
    debug = os.environ.get('DASH_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=8050, debug=debug)
