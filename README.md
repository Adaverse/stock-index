# stock-index

## Installation

To set up the project locally, follow these steps:

1. **Install Poetry**  
   This project uses [Poetry](https://python-poetry.org/) for dependency management. If you don't have Poetry installed, you can install it using `pipx`:

   ```bash
   pipx install poetry
2. **Clone the Repository**  
   Clone this repository to your local machine:

   ```bash
   git clone https://github.com/Adaverse/stock-index.git
   cd stock-index
3. **Install Dependencies**  
   Use Poetry to install the project dependencies:

   ```bash
   poetry install
4. **Initialize DB**  
   To import data from API and initialize required tables, run below command (num-tickers is optional field which specifies how many tickers data to fetch from API. For quick testing it can be set to small values like 50):

   ```bash
   poetry run python top_100_stock_index/cli.py init-data --num-tickers 50
5. **Initialize INDEX**  
   After successful data import, we can initialize index data along with the required calculation from start till current date for top x stock by market cap with first argument being ticker name for index and second argument being the top_x to pick by market cap: 
   ```bash
   poetry run python top_100_stock_index/cli.py init-or-update-index I25 25
6. **Start UI**  
   To start the streamlit UI, run below command:
   ```bash
   poetry run streamlit run top_100_stock_index/ui/main.py

For any cli related help, run
   ```bash
   poetry run python top_100_stock_index/cli.py --help
