# ABOUT
This project contains the code for data scraping, data cleaning, and modeling used to generate forecasts shown on [econforecasting.com](https://econforecasting.com).


---
# CHANGELOG

## 2021-02-20 [v0.10]
- Added import spreadsheet
- Added GDP subcomponents
- Added deseasonalization information
- Added first monthly interpolation
- Added Kalman filtration code
- Added PCA analysis
- Added dynamic factor models
- Added documentation for Kalman filters

## 2021-02-13 [v0.09]
- Added mortgage models
- Added misc stationary transform functions for structural modeling
- Added addLags() function
- Added historical data aggregation for SEMs
- Added expected inflation models
- Added prep for Kalman smoothing & interpolation

## 2021-02-09 [v0.08]
- Added monthly aggregation for daily-data forecasts
- Added SOFR web scraping
- Fixed some issues with inconsistent variable names

## 2021-02-03 [v0.05] (Prepping Forecasts For Website Display)
- Added dynamic Nelson-Siegel interest rate decomposition & forecasting model
- Added code to allow for SPF pulling for more variables
- Added code to pull historical data of multiple frequencies
- Added SQL inserts to fc tables
- Added SQL insert function
- Improved overall code organization in forecasts.rmd

## 2021-01-29 [v0.04]
- Added historical data scraping code
- Added code for fed funds rate
- Fixed bug in data upload
- Fixed bug in getDataFred()
- Fixed bug in package description compilation

## 2021-01-13 [v0.03]
- Added code for calculating asset contagion index
- Added code for splitting RESET_ALL runs from update runs
- Added code for truncating tables on RESET_ALL runs

## 2021-01-09 [v0.02]
- Added code for remote SQL connection
- Added code for calculation of cross-asset rolling correlation statistics
- Added code for remote SFTP and COPY batch insert of asset-correlation data

## 2021-01-08 [v0.01]
- Added web scraping code for CBO
- Added web scraping code for FRED
- Added web scraping code for WSJ
- Added web scraping code for Philadelphia Fed 

## 2021-01-07 [v0.00]
- Added initial package setup
- Added initial web scraping frameworks etup
