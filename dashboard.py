import time
import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# إعدادات الصفحة
st.set_page_config(
    page_title="لوحة تحليل حوادث المرور لايف", layout="wide"
)
st.title("🚦 لوحة متابعة وتحليل حوادث المرور اللحظية (Real-Time)")


# دالة الاتصال بـ PostgreSQL (تأكد من مطابقة الباسورد واسم الداتابيز عندك)
def get_data(query):
  conn = psycopg2.connect(
      host="localhost",
      port="5432",
      database="traffic_db",
      user="yahya",
      password="yahya2026pass",
  )
  df = pd.read_sql(query, conn)
  conn.close()
  return df


placeholder = st.empty()
iteration = 0

while True:
  iteration += 1
  with placeholder.container():
    # 1. سحب البيانات المجمعة من الـ 5 جداول
    geo_df = get_data("""
            SELECT city, state, SUM(accident_count) AS total_accidents, ROUND(AVG(avg_severity)::numeric, 2) AS avg_severity
            FROM geo_accidents GROUP BY city, state ORDER BY total_accidents DESC;
        """)
    weather_df = get_data("""
            SELECT weather_condition, road_condition, SUM(accident_count) AS total_accidents
            FROM weather_road_accidents GROUP BY weather_condition, road_condition ORDER BY total_accidents DESC;
        """)
    factor_df = get_data("""
            SELECT contributing_factor, SUM(accident_count) AS total_accidents
            FROM factor_accidents GROUP BY contributing_factor ORDER BY total_accidents DESC;
        """)
    vehicle_df = get_data("""
            SELECT vehicle_type, SUM(accident_count) AS total_accidents
            FROM vehicle_accidents GROUP BY vehicle_type ORDER BY total_accidents DESC;
        """)
    time_df = get_data("""
            SELECT accident_hour, SUM(accident_count) AS total_accidents
            FROM time_accidents GROUP BY accident_hour ORDER BY accident_hour;
        """)

    # 2. عرض المؤشرات السريعة فوق (KPIs)
    total_all = (
        int(geo_df["total_accidents"].sum()) if not geo_df.empty else 0
    )
    top_city = geo_df.iloc[0]["city"] if not geo_df.empty else "لا يوجد"
    top_factor = (
        factor_df.iloc[0]["contributing_factor"]
        if not factor_df.empty
        else "لا يوجد"
    )

    k1, k2, k3 = st.columns(3)
    k1.metric("إجمالي الحوادث المسجلة", total_all)
    k2.metric("أكثر مدينة تسجيلاً للحوادث", top_city)
    k3.metric("السبب الرئيسي للحوادث", top_factor)

    st.divider()

    # 3. الصف الأول: التوزيع الجغرافي وأسباب الحوادث
    col1, col2 = st.columns(2)
    with col1:
      st.subheader("📍 الحوادث حسب المدينة")
      if not geo_df.empty:
        fig_geo = px.bar(
            geo_df,
            x="city",
            y="total_accidents",
            color="avg_severity",
            text_auto=True,
            labels={
                "city": "المدينة",
                "total_accidents": "إجمالي الحوادث",
                "avg_severity": "متوسط الخطورة",
            },
        )
        st.plotly_chart(
            fig_geo, use_container_width=True, key=f"geo_chart_{iteration}"
        )

    with col2:
      st.subheader("⚠️ أسباب الحوادث")
      if not factor_df.empty:
        fig_factor = px.pie(
            factor_df,
            names="contributing_factor",
            values="total_accidents",
            hole=0.4,
            labels={
                "contributing_factor": "سبب الحادث",
                "total_accidents": "إجمالي الحوادث",
            },
        )
        st.plotly_chart(
            fig_factor,
            use_container_width=True,
            key=f"factor_chart_{iteration}",
        )

    # 4. الصف الثاني: حالة الطقس والطريق وأنواع المركبات
    col3, col4 = st.columns(2)
    with col3:
      st.subheader("🌦️ حالة الطقس والطريق")
      if not weather_df.empty:
        fig_weather = px.bar(
            weather_df,
            x="weather_condition",
            y="total_accidents",
            color="road_condition",
            barmode="group",
            labels={
                "weather_condition": "حالة الطقس",
                "total_accidents": "إجمالي الحوادث",
                "road_condition": "حالة الطريق",
            },
        )
        st.plotly_chart(
            fig_weather,
            use_container_width=True,
            key=f"weather_chart_{iteration}",
        )

    with col4:
      st.subheader("🚗 أنواع المركبات")
      if not vehicle_df.empty:
        fig_veh = px.bar(
            vehicle_df,
            x="vehicle_type",
            y="total_accidents",
            color="vehicle_type",
            text_auto=True,
            labels={
                "vehicle_type": "نوع المركبة",
                "total_accidents": "إجمالي الحوادث",
            },
        )
        st.plotly_chart(
            fig_veh,
            use_container_width=True,
            key=f"vehicle_chart_{iteration}",
        )

    # 5. الصف الثالث: توزيع الحوادث على مدار ساعات اليوم
    st.subheader("⏰ توزيع الحوادث حسب ساعات اليوم")
    if not time_df.empty:
      fig_time = px.line(
          time_df,
          x="accident_hour",
          y="total_accidents",
          markers=True,
          labels={
              "accident_hour": "الساعة (0 - 23)",
              "total_accidents": "إجمالي الحوادث",
          },
      )
      st.plotly_chart(
          fig_time, use_container_width=True, key=f"time_chart_{iteration}"
      )

  # تحديث الشاشة كل 5 ثواني
  time.sleep(5)