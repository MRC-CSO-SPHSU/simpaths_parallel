# Runs 1,000 runs as 50 sequential runs of n=20 with 50 unique starting seeds 
# Args:
# -r: random seed (passed as 50 unique starting points at end of call)
# -n: number of times to run within each parallel instance (here, 20*50 = 1,000 runs)
# -p: population size to simulate
# -s: start year - this must be equal to or greater than the earliest population provided in the `input` folder
# -e: end year - equal to or greater than the start year. No known upper limit
# -g: show gui? Here set to false to immediately begin headless runs
# -f: write console outputs and logs to file (stored in `output/logs`)

parallel java -cp simpaths.jar simpaths.experiment.SimPathsMultiRun -r {} -n 20 -p 150000 -s 2017 -e 2025 -g false -f ::: {100..5000..100}

# Combine csvs to parquet files in 'reform_sens' folder (change to new folder)

Rscript R/combining_arrow_data.R reform_sens

# Tidy output folders, removing empty database folders and redundant input folders (keeps output data)

rm -r output/2023*/database
rm -r output/2023*/input

# Compress to tar.zip archive using pigzip
# Note: change zip file name to match run

tar -cv -I 'pigz --zip' -f /storage/andy/simpaths_outputs/csvs/reform_sens_outputs.tar.zip /storage/andy/simpaths_outputs/output

# Text myself that it's all done
# Environment variables:
#  -Notify_bot_key and telegram_chatid: generated for personalised notification bot

curl "https://api.telegram.org/bot${Notify_bot_key}/sendMessage?text=Done%20copying%20all%20files&chat_id=${telegram_chatid}"
