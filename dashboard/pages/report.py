from dash import html, callback, Input, Output
from datetime import date, timedelta

from data import build_report_table, humanize_alert


def _trend_cell(value, arrow, color) -> html.Td:
    if value == '-':
        return html.Td('-')
    return html.Td([
        html.Span(f"{value}  "),
        html.Span(arrow, style={'color': color, 'fontWeight': 'bold'}),
    ])


def _alert_count_cell(count: int) -> html.Td:
    color = 'red' if count > 0 else 'green'
    return html.Td(str(count), style={'color': color, 'fontWeight': 'bold'})


def _table(rows: list) -> html.Table:
    header = html.Tr([
        html.Th('Champ'),
        html.Th('Temp. moy. (°C)'),
        html.Th('Humidité moy. (%)'),
        html.Th('Azote moy. (mg/kg)'),
        html.Th('Alertes'),
    ])

    body_rows = []
    for r in rows:
        body_rows.append(html.Tr([
            html.Td(r['name']),
            _trend_cell(r['temp'], r['temp_arrow'], r['temp_color']),
            _trend_cell(r['humidity'], r['humidity_arrow'], r['humidity_color']),
            _trend_cell(r['nitrogen'], r['nitrogen_arrow'], r['nitrogen_color']),
            _alert_count_cell(r['nb_alertes']),
        ]))

    return html.Table([html.Thead(header), html.Tbody(body_rows)], className='report-table')


def layout() -> html.Div:
    return html.Div([
        html.Div(id='report-content'),
    ])


def register_callbacks(app):
    @app.callback(
        Output('report-content', 'children'),
        Input('store-report-today', 'data'),
        Input('store-report-yesterday', 'data'),
    )
    def update_report(today, yesterday):
        if not today:
            return html.P('Rapport non disponible — les données seront chargées dans quelques instants.', className='no-data')

        report_date = today.get('date', str(date.today()))
        rows = build_report_table(today, yesterday)

        header_parts = [html.Span(f"Rapport du {report_date}", className='report-date')]

        cultures_recolte = today.get('cultures_recolte_imminente', [])
        if cultures_recolte:
            header_parts.append(html.Span('🌻 Prêt à récolter', className='badge badge-green'))

        yesterday_note = None
        if not yesterday:
            yesterday_note = html.P('Données du jour précédent non disponibles.', className='note')

        total_alertes = today.get('total_alertes', {})
        alert_summary = None
        if total_alertes:
            pills = [
                html.Span(f"{humanize_alert(code)} : {count}", className='alert-pill')
                for code, count in total_alertes.items()
            ]
            alert_summary = html.Div([
                html.H4('Résumé des alertes du jour'),
                html.Div(pills, className='alert-pills'),
            ], className='alert-summary')

        return html.Div([
            html.Div(header_parts, className='report-header'),
            _table(rows),
            yesterday_note,
            alert_summary,
        ])
