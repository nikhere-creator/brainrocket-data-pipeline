-- BrainRocket Gaming Data Pipeline - Seed Data
-- Populate dimension tables with sample data

-- Insert sample games
INSERT INTO dim_game (game_name, game_genre, release_date, developer, price, is_active) VALUES
('Cyber Quest', 'RPG', '2023-01-15', 'NeoGames', 49.99, TRUE),
('Stellar Conquest', 'Strategy', '2023-03-22', 'SpaceDev', 39.99, TRUE),
('Racing Extreme', 'Racing', '2023-05-10', 'SpeedStudios', 29.99, TRUE),
('Mystic Legends', 'Adventure', '2023-02-28', 'FantasyWorks', 59.99, TRUE),
('Battle Royale Arena', 'Action', '2023-04-05', 'ActionLabs', 0.00, TRUE), -- Free-to-play
('Puzzle Masters', 'Puzzle', '2023-06-18', 'BrainGames', 19.99, TRUE),
('Sports Championship', 'Sports', '2023-07-12', 'SportSim', 49.99, TRUE),
('Retro Arcade', 'Arcade', '2023-08-25', 'ClassicGames', 9.99, TRUE),
('Survival Island', 'Survival', '2023-09-30', 'WildStudios', 34.99, TRUE),
('Fantasy Kingdom', 'Simulation', '2023-10-15', 'SimWorks', 44.99, TRUE);

-- Insert sample locations
INSERT INTO dim_location (country_code, country_name, region, timezone, currency_code) VALUES
('US', 'United States', 'North America', 'America/New_York', 'USD'),
('CA', 'Canada', 'North America', 'America/Toronto', 'CAD'),
('GB', 'United Kingdom', 'Europe', 'Europe/London', 'GBP'),
('DE', 'Germany', 'Europe', 'Europe/Berlin', 'EUR'),
('FR', 'France', 'Europe', 'Europe/Paris', 'EUR'),
('JP', 'Japan', 'Asia', 'Asia/Tokyo', 'JPY'),
('AU', 'Australia', 'Oceania', 'Australia/Sydney', 'AUD'),
('BR', 'Brazil', 'South America', 'America/Sao_Paulo', 'BRL'),
('IN', 'India', 'Asia', 'Asia/Kolkata', 'INR'),
('CN', 'China', 'Asia', 'Asia/Shanghai', 'CNY'),
('RU', 'Russia', 'Europe', 'Europe/Moscow', 'RUB'),
('MX', 'Mexico', 'North America', 'America/Mexico_City', 'MXN'),
('ZA', 'South Africa', 'Africa', 'Africa/Johannesburg', 'ZAR'),
('KR', 'South Korea', 'Asia', 'Asia/Seoul', 'KRW'),
('SG', 'Singapore', 'Asia', 'Asia/Singapore', 'SGD');

-- Generate sample transactions (this will be populated by ETL and streaming)
-- Note: The fact_transactions table will be populated by the data generator and ETL processes
