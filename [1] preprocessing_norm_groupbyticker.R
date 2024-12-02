# Set working directory
setwd("C:/Users/giyan/OneDrive - Tilburg University/03. Master Data Science/06. Thesis")

# Load required libraries
library(readxl)
library(dplyr)
library(dplyr)
library(lubridate)
library(tidyr)
library(caret)
library(scales)
library(purrr)

# Loading the forex data
EURUSD_minute <- read_excel("data/eurusd_minute.xlsx")
AUDUSD_minute <- read_excel("data/audusd_minute.xlsx")
EURCHF_minute <- read_excel("data/eurchf_minute.xlsx")
GBPUSD_minute <- read_excel("data/gbpusd_minute.xlsx")
USDJPY_minute <- read_excel("data/usdjpy_minute.xlsx")

# Loading the sentiment data
sentiment_prediction_raw <- read.csv("data/sentiment_predictions_single_article.csv")

# Combine all forex data
forex_combined <- bind_rows(EURUSD_minute, AUDUSD_minute, EURCHF_minute, GBPUSD_minute, USDJPY_minute)

# Ensure the 'Date' column is in datetime format
forex_combined$Date <- ymd_hms(forex_combined$Date)




# Group by 'Ticker' and make opening price, closing price, highest price, and lowest price. 
forex_hourly <- forex_combined %>%
  mutate(Date = floor_date(Date, unit = "hour")) %>%
  group_by(Ticker, Date) %>%
  summarise(closing_price = last(Price), 
            opening_price = first(Price), 
            high = max(Price), 
            low = min(Price)) %>%
  ungroup()

# Remove non-trading hours based on Forex trading hours
forex_hourly <- forex_hourly %>%
  # Filter out Saturdays entirely
  filter(!(weekdays(Date) %in% c("Saturday"))) %>%
  # Keep only hours from Sunday 5 p.m. onward
  filter(!(weekdays(Date) == "Sunday" & hour(Date) < 17)) %>%
  # Remove hours after 5 p.m. on Fridays
  filter(!(weekdays(Date) == "Friday" & hour(Date) >= 17))



# Define the end date (2023-05-04), There are no articles after this date
end_date <- ymd("2023-05-04")

# Filter the forex_hourly dataset to keep data up to 2023-05-04
forex_hourly_filtered <- forex_hourly %>%
  filter(Date <= end_date)


sum(is.na(forex_hourly))


#preprocessing sentiment_prediction


#only selecting useful columns. 

sentiment_prediction <- sentiment_prediction_raw %>%
  select(published_at, ticker, gpt_sentiment_p7n)




# Some articles are written over the weekend, when the FX pairs are not traded. Therefore I assign these to the next trading hour. 

# Ensure 'published_at' is in datetime format before filtering
sentiment_prediction <- sentiment_prediction %>%
  mutate(published_at = mdy_hm(published_at))  # Convert to datetime using Month-Day-Year Hour:Minute format

# Step 1: Filter out weekend rows (Saturday and Sunday before 5 p.m.)
weekend_sentiment <- sentiment_prediction %>%
  filter(weekdays(published_at) %in% c("Saturday", "Sunday") & hour(published_at) < 17) %>%  # Weekend articles before Sunday 5 p.m.
  mutate(weekend_identifier = floor_date(published_at, "week", week_start = 6))  # Create a weekend identifier starting from Saturday

# Step 2: Group by ticker and weekend_identifier, and calculate the average sentiment
weekend_sentiment_avg <- weekend_sentiment %>%
  group_by(ticker, weekend_identifier) %>%
  summarise(weekend_avg_sentiment = mean(gpt_sentiment_p7n, na.rm = TRUE))  # Average sentiment for the weekend

# Step 3: Find the first trading hour after the weekend (Sunday 5 p.m. or later)
first_trading_hour <- sentiment_prediction %>%
  filter(weekdays(published_at) == "Sunday" & hour(published_at) >= 17) %>%  # Sunday 5 p.m. or later
  group_by(ticker) %>%
  slice(1)  # First available trading hour for each ticker after the weekend

# Step 4: Merge the weekend average sentiment with the first trading hour
updated_sentiment <- first_trading_hour %>%
  left_join(weekend_sentiment_avg, by = c("ticker")) %>%
  mutate(gpt_sentiment_p7n = ifelse(is.na(gpt_sentiment_p7n), weekend_avg_sentiment, gpt_sentiment_p7n))  # Apply the average


#Step 5: Remove unnecessary columns before updating sentiment_prediction
updated_sentiment_clean <- updated_sentiment %>%
  select(ticker, published_at, gpt_sentiment_p7n) %>%  # Keep only the relevant columns
  distinct()

# Step 6: Update the sentiment_prediction dataset with the new averaged weekend sentiment
sentiment_prediction_updated <- sentiment_prediction %>%
  rows_update(updated_sentiment_clean, by = c("ticker", "published_at"))

# Display the updated dataset
print(sentiment_prediction_updated)




sentiment_prediction_updated <- sentiment_prediction_updated %>%
  mutate(Date = floor_date(published_at, unit = "hour"))  # Round to the nearest hour




# Step 2: Group by Ticker and Date, and calculate the average of gpt_sentiment_p7n for each hour
sentiment_hourly_avg <- sentiment_prediction_updated %>%
  group_by(ticker, Date) %>%
  summarise(gpt_sentiment = mean(gpt_sentiment_p7n, na.rm = TRUE))  # Calculate the average sentiment for each hour





# Perform a left join on Ticker and Date
merged_data <- sentiment_hourly_avg %>%
  right_join(forex_hourly, by = c("ticker" = "Ticker", "Date" = "Date"))


# Replace NA gpt_sentiment values with 0 (neutral sentiment)
merged_data <- merged_data %>%
  mutate(gpt_sentiment = ifelse(is.na(gpt_sentiment), 0, gpt_sentiment))


summary(merged_data)



write.csv(merged_data, file = "merged_data", row.names = F)



### ----- Setting up the features

# 1. Calculating Hourly returns

merged_data <- merged_data %>%
  arrange(Date) %>%  # Ensure the data is sorted by date
  mutate(returns = (closing_price / lag(closing_price) - 1))

# 2. Create binary target based on returns
merged_data <- merged_data %>%
  mutate(binary_target = ifelse(returns > 0, 1, 0))

merged_data <- merged_data %>%
  select(c(-opening_price, -high, -low))


# first data points dont have price t-1 -> gives NA -> need to remove those
merged_data <- merged_data %>%
  na.omit()






### 3. Adding lagged features in DF
#make sure it lags per ticker
merged_data <- merged_data %>%
  group_by(ticker)


# Define a function to generate multiple lagged columns
create_lags <- function(data, var, max_lag) {
  lagged_vars <- map(1:max_lag, ~lag(data[[var]], .x))
  colnames(lagged_vars) <- paste0(var, "_lag_", 1:max_lag)
  return(as.data.frame(lagged_vars))
}

# Create lagged variables for returns (240 lags)
for (i in 1:240) {
  merged_data <- merged_data %>%
    mutate(!!paste0("lagged_return_", i) := lag(returns, i))
}

# Create lagged variables for sentiment (24 lags)
for (i in 1:24) {
  merged_data <- merged_data %>%
    mutate(!!paste0("lagged_sentiment_", i) := lag(gpt_sentiment, i))
}

#this creates NA's for the first 240 dates
merged_data <- merged_data %>%
  na.omit() %>%
  ungroup()



### 4. Splitting the data in train/val/test

# dont need closing price anymore -> drop this

merged_data <- merged_data %>%
  select(-closing_price)

# Define date ranges for 70/15/15 split
train_end <- as.POSIXct("2023-09-14 23:59:59")
val_start <- as.POSIXct("2023-09-15 00:00:00")
val_end <- as.POSIXct("2023-11-07 23:59:59")
test_start <- as.POSIXct("2023-11-08 00:00:00")

# Split the data by date
train_data <- merged_data %>% filter(Date <= train_end)
val_data <- merged_data %>% filter(Date >= val_start & Date <= val_end)
test_data <- merged_data %>% filter(Date >= test_start)

#### 5. Normalizing the data
# Min-max normalization function
normalize <- function(x, min_val, max_val) {
  return((x - min_val) / (max_val - min_val))
}

# Normalize the training set by ticker
train_data <- train_data %>%
  group_by(ticker) %>%
  mutate(
    min_return = min(returns, na.rm = TRUE),
    max_return = max(returns, na.rm = TRUE),
    returns = normalize(returns, min_return, max_return),
    across(starts_with("lagged_return"), ~normalize(., min_return, max_return)),
    across(starts_with("lagged_sentiment"), ~normalize(., 0, 1)) # Assuming sentiment is between 0 and 1
  ) %>%
  ungroup() %>% # Remove grouping after normalization
  select(-min_return, -max_return) # Drop temporary min and max columns

# Normalize the validation set by ticker using training set min and max per ticker
val_data <- val_data %>%
  left_join(train_data %>% select(ticker, min_return, max_return) %>% distinct(), by = "ticker") %>%
  mutate(
    returns = normalize(returns, min_return, max_return),
    across(starts_with("lagged_return"), ~normalize(., min_return, max_return)),
    across(starts_with("lagged_sentiment"), ~normalize(., 0, 1)) # Assuming sentiment is between 0 and 1
  ) %>%
  select(-min_return, -max_return) # Drop temporary min and max columns

# Normalize the test set by ticker using training set min and max per ticker
test_data <- test_data %>%
  left_join(train_data %>% select(ticker, min_return, max_return) %>% distinct(), by = "ticker") %>%
  mutate(
    returns = normalize(returns, min_return, max_return),
    across(starts_with("lagged_return"), ~normalize(., min_return, max_return)),
    across(starts_with("lagged_sentiment"), ~normalize(., 0, 1)) # Assuming sentiment is between 0 and 1
  ) %>%
  select(-min_return, -max_return) # Drop temporary min and max columns

# After normalization, keep min_return and max_return in the datasets
train_data <- train_data %>% select(-min_return, -max_return)
val_data <- val_data %>% select(-min_return, -max_return)
test_data <- test_data %>% select(-min_return, -max_return)



# Save training data to CSV
write.csv(train_data, "data/train_data_v1.csv", row.names = FALSE)

# Save validation data to CSV
write.csv(val_data, "data/val_data_v1.csv", row.names = FALSE)

# Save test data to CSV
write.csv(test_data, "data/test_data_v1.csv", row.names = FALSE)


