import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import io
from prophet import Prophet
import matplotlib.dates as mdates

# подключение к базе
conn = psycopg2.connect(
    dbname="prices_db",
    user="postgres",
    password="Raidraid27_",
    host="localhost",
    port="5432"
)



@st.cache_data
def load_data():
    query = """
        SELECT p.name AS product, c.name AS city, year, month, price
        FROM product_prices pp
        JOIN products p ON p.id = pp.product_id
        JOIN cities c ON c.id = pp.city_id
        ORDER BY year, month
    """
    df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    return df

df = load_data()

# фильтры
st.title("📊 Анализ цен на продукты")
exclude = set([
    'Рожки(весовые)3)',
    'Рис шлифованный, полированный(весовой) 3)',
    'Крупа гречневая (весовая) 3)',
    'Рожки(весовые)',
    'Рис шлифованный, полированный (весовой)',
    'Крупа гречневая (весовая)'
])
valid_products = [p for p in df['product'].dropna().unique() if p not in exclude]
product = st.selectbox("Выберите продукт", sorted(valid_products))

city = st.selectbox("Выберите город", df['city'].unique())

filtered = df[(df['product'] == product) & (df['city'] == city)]


#  График динамики цен
st.subheader("📈 Динамика цен")
fig, ax = plt.subplots()

# линия + точки
ax.plot(filtered['date'], filtered['price'], marker='o', color='blue', alpha=0.7)

# форматирование оси икс
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
fig.autofmt_xdate()

# оформление
ax.set_title(f"{product} — {city}")
ax.set_ylabel("Цена (тг)")
ax.set_xlabel("Дата")
ax.grid(True)
ax.legend(["Цена"], loc="upper left")

st.pyplot(fig)


# скачать график
buf = io.BytesIO()
fig.savefig(buf, format="png")
st.download_button(
    label="📥 Скачать график (PNG)",
    data=buf.getvalue(),
    file_name=f"{product}_{city}_prices.png",
    mime="image/png",
)

#  скачать данные
csv = filtered.to_csv(index=False).encode('utf-8')
st.download_button(
    label="📥 Скачать данные (CSV)",
    data=csv,
    file_name=f"{product}_{city}_prices.csv",
    mime="text/csv",
)

#  прогноз цен
st.subheader("🔮 Прогноз на 6 месяцев")
if len(filtered) >= 6:
    model_df = filtered[['date', 'price']].rename(columns={'date': 'ds', 'price': 'y'})
    model = Prophet()
    model.fit(model_df)
    future = model.make_future_dataframe(periods=6, freq='M')
    forecast = model.predict(future)

    fig2, ax2 = plt.subplots()
    ax2.plot(model_df['ds'], model_df['y'], label='Исторические данные', color='blue', alpha=0.6)
    ax2.plot(forecast['ds'], forecast['yhat'], label='Прогноз', color='orange', linestyle='--', alpha=0.8)

    # форматирование оси X
    import matplotlib.dates as mdates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))   
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))   
    fig2.autofmt_xdate()                                          

    #  ограничение по времени
    ax2.set_xlim([model_df['ds'].min(), pd.to_datetime('2025-12-01')])

    ax2.set_title("Прогноз цен")
    ax2.set_ylabel("Цена (тг)")
    ax2.set_xlabel("Дата")
    ax2.grid(True)
    ax2.legend()
    st.pyplot(fig2)
else:
    st.warning("Недостаточно данных для прогноза (нужно минимум 6 точек).")


# сравнение городов
st.subheader("🏙️ Сравнение городов")
multi_city = df[df['product'] == product]
pivot = multi_city.pivot_table(index='date', columns='city', values='price')
st.line_chart(pivot)

