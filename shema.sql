-- 1. جدول التوزيع الجغرافي للحوادث
CREATE TABLE IF NOT EXISTS geo_accidents (
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    accident_count BIGINT DEFAULT 0,
    avg_severity DOUBLE PRECISION DEFAULT 0.0,
    PRIMARY KEY (city, state)
);

-- 2. جدول التوزيع الزمني للحوادث
CREATE TABLE IF NOT EXISTS time_accidents (
    accident_hour INT NOT NULL,
    day_of_week INT NOT NULL,
    accident_month INT NOT NULL,
    accident_year INT NOT NULL,
    accident_count BIGINT DEFAULT 0,
    PRIMARY KEY (accident_hour, day_of_week, accident_month, accident_year)
);

-- 3. جدول حالة الطقس والطريق
CREATE TABLE IF NOT EXISTS weather_road_accidents (
    weather_condition VARCHAR(100) NOT NULL,
    road_condition VARCHAR(100) NOT NULL,
    accident_count BIGINT DEFAULT 0,
    PRIMARY KEY (weather_condition, road_condition)
);

-- 4. جدول الأسباب الرئيسية للحوادث
CREATE TABLE IF NOT EXISTS factor_accidents (
    contributing_factor VARCHAR(255) NOT NULL,
    accident_count BIGINT DEFAULT 0,
    PRIMARY KEY (contributing_factor)
);

-- 5. جدول أنواع المركبات
CREATE TABLE IF NOT EXISTS vehicle_accidents (
    vehicle_type VARCHAR(100) NOT NULL,
    accident_count BIGINT DEFAULT 0,
    PRIMARY KEY (vehicle_type)
);