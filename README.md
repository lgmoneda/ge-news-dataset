# GE News Dataset
Script and notebook to update the GE News dataset (https://www.kaggle.com/datasets/lgmoneda/ge-soccer-clubs-news)

## Dependencies

Not all dependencies in the 'environment.yml' are needed, it is just the environment I used to run it. It uses Selenium's Firefox.

## Data

Define the 'SAVING_PATH' in the 'extract.py'. It will save individual articles as csv files under a folders with the team names.

## Extracting info

### Update the most new news

`python extract.py update <update_pages>`

`update_pages`: the number of pages to scrap for all the teams.

## Extract randomly from all teams

`python extract.py update <update_pages> <n_hits>`

`update_pages`: the max page to scrap for all the teams.

`n_hits`: how many random pages for every team are going to be extracted.

## Single team extraction

`python extract.py <team_name> <update_pages> <n_hits>`

E.g. `python extract sao-paulo 100 10` is going to extract 10 news pages from 1-100 randomly.

`team_name`: the club name as it appears in the club column.

`update_pages`: the max page to scrap for all the teams.

`n_hits`: how many random pages for every team are going to be extracted.

## Generating the dataset

Run the 'aggregating_data.ipynb'.
