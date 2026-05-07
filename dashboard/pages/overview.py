from dash import html, dcc, Input, Output, State, ctx, ALL
import plotly.graph_objects as go

from data import build_field_cards, build_detail, NPK_THRESHOLDS, SENSOR_NAMES


def _card(card: dict) -> html.Div:
    sensor_id = card['sensor_id']
    if card.get('no_data'):
        return html.Div(
            id={'type': 'field-card', 'index': sensor_id},
            children=[
                html.Div(f"{card['emoji']} {card['name']}", className='card-title'),
                html.P('Aucune donnée récente', className='no-data'),
            ],
            className='field-card no-data-card',
            n_clicks=0,
        )

    color = card['status_color']
    alerts_items = [html.Li(a) for a in card['alerts']] if card['alerts'] else []

    return html.Div(
        id={'type': 'field-card', 'index': sensor_id},
        children=[
            html.Div([
                html.Span(f"{card['emoji']} {card['name']}", className='card-title'),
                html.Span(card['status'], className=f'badge badge-{color}'),
            ], className='card-header'),
            html.Div([
                html.Span(f"🌡 {card['temp']}°C"),
                html.Span(f"💧 {card['humidity']}%"),
            ], className='card-metrics'),
            html.P(card['stage'], className='card-stage'),
            html.Ul(alerts_items, className='card-alerts') if alerts_items else None,
        ],
        className='field-card',
        n_clicks=0,
    )


def _npk_gauge(label: str, value: float, threshold: float) -> html.Div:
    ok = value >= threshold
    pct = min(value / threshold * 100, 100) if threshold else 0
    status_text = '✓ Correct' if ok else '⚠ Trop bas'
    bar_color = 'green' if ok else 'red'
    return html.Div([
        html.Div([
            html.Span(label, className='gauge-label'),
            html.Span(f"{value} mg/kg", className='gauge-value'),
            html.Span(status_text, className=f'gauge-status gauge-{bar_color}'),
        ], className='gauge-header'),
        html.Div(
            html.Div(style={
                'width': f'{pct:.0f}%',
                'background': '#27ae60' if ok else '#e74c3c',
                'height': '100%',
                'borderRadius': '4px',
            }),
            className='gauge-bar',
        ),
    ], className='gauge')


def _detail_panel(detail: dict, sensor_id: str) -> html.Div:
    if not detail:
        return html.Div(
            'Sélectionnez un champ pour voir le détail.',
            className='detail-placeholder',
        )

    name, emoji = SENSOR_NAMES.get(sensor_id, (sensor_id, ''))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=detail['timestamps'], y=detail['temperatures'],
        name='Température (°C)', line={'color': '#e74c3c', 'width': 2},
    ))
    fig.add_trace(go.Scatter(
        x=detail['timestamps'], y=detail['humidities'],
        name='Humidité (%)', line={'color': '#2980b9', 'width': 2},
        yaxis='y2',
    ))
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin={'t': 20, 'b': 30, 'l': 50, 'r': 60},
        legend={'orientation': 'h', 'y': -0.2},
        yaxis={'title': 'Temp. (°C)', 'gridcolor': '#eef0f3'},
        yaxis2={'title': 'Humidité (%)', 'overlaying': 'y', 'side': 'right'},
        height=260,
    )

    alert_items = [
        html.Li(f"{label} — {count} occurrence(s)")
        for label, count in detail['alert_counts'].items()
    ]

    stress_color = detail['stress_color']

    return html.Div([
        html.H3(f"{emoji} {name} — Détail", className='detail-title'),
        dcc.Graph(figure=fig, config={'displayModeBar': False}),
        html.Div([
            _npk_gauge('Azote', detail['nitrogen'], NPK_THRESHOLDS['nitrogen']),
            _npk_gauge('Phosphore', detail['phosphorus'], NPK_THRESHOLDS['phosphorus']),
            _npk_gauge('Potassium', detail['potassium'], NPK_THRESHOLDS['potassium']),
        ], className='npk-row'),
        html.Div([
            html.Span('Score de stress : ', className='stress-prefix'),
            html.Span(
                f"{detail['stress_score']} / 100 — {detail['stress_label']}",
                className=f'stress-value stress-{stress_color}',
            ),
        ], className='stress-row'),
        (html.Ul(alert_items, className='detail-alerts')
         if alert_items
         else html.P('Aucune alerte active.', className='no-alert')),
    ], className='detail-panel')


def layout() -> html.Div:
    return html.Div([
        dcc.Store(id='selected-field', data=None),
        html.Div(id='alert-banner'),
        html.Div(id='field-cards-grid', className='cards-grid'),
        html.Div(id='detail-section'),
    ])


def register_callbacks(app):
    @app.callback(
        Output('field-cards-grid', 'children'),
        Output('alert-banner', 'children'),
        Input('store-sensor', 'data'),
        Input('selected-field', 'data'),
    )
    def update_cards(store_data, selected):
        import pandas as pd
        df = pd.DataFrame(store_data) if store_data else pd.DataFrame()
        cards = build_field_cards(df)

        all_alerts = []
        for c in cards:
            if not c.get('no_data'):
                for a in c.get('alerts', []):
                    all_alerts.append(f"{a} ({c['name']})")

        banner = None
        if all_alerts:
            banner = html.Div(
                f"🚨 {len(all_alerts)} alerte(s) active(s) — " + ' · '.join(all_alerts),
                className='alert-banner',
            )

        card_els = []
        for c in cards:
            el = _card(c)
            if c['sensor_id'] == selected:
                el.className = (el.className or '') + ' selected'
            card_els.append(el)

        return card_els, banner

    @app.callback(
        Output('selected-field', 'data'),
        Input({'type': 'field-card', 'index': ALL}, 'n_clicks'),
        State({'type': 'field-card', 'index': ALL}, 'id'),
        prevent_initial_call=True,
    )
    def select_field(n_clicks_list, ids):
        if not ctx.triggered_id or not isinstance(ctx.triggered_id, dict):
            return None
        return ctx.triggered_id['index']

    @app.callback(
        Output('detail-section', 'children'),
        Input('selected-field', 'data'),
        Input('store-sensor', 'data'),
    )
    def update_detail(selected, store_data):
        import pandas as pd
        df = pd.DataFrame(store_data) if store_data else pd.DataFrame()
        if not selected:
            return html.Div(
                'Cliquez sur un champ pour voir le détail.',
                className='detail-placeholder',
            )
        detail = build_detail(df, selected)
        return _detail_panel(detail, selected)
