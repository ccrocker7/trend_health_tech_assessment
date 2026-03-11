# Pick & Profile A Data Source
I have selected the hospital pricing transparency file from the hospital that I was born at. This selection was made to illustrate my ability to work with hospital pricing data given it's relevance to your product. The hospital pricing transparency requirements from CMS can be found here: https://www.cms.gov/files/document/august-11-2021-hospital-price-transparency-odf-slide-presentation.pdf. I found the specific data being ingested and analyzed in this project here: https://hospitalpricingfiles.org/details/a37eb62f-b584-41ee-87b6-e85de2faee70.

# Design a SQLite Schema
![Schema Diagram](schema_diagram.png)

# Ingest the data
These instructions should set up a copy of the project and get it running on your local Windows machine for review. Feel free to reach out if anything appears to be amiss.
## Prerequisites
~~~
    Python 3.10+ (Ensure it is added to your PATH).
    Git (for version control).
~~~

## Repository Initialization
### Clone the repository:
~~~
    git clone https://github.com/your-username/your-repo-name.git
    cd your-repo-name
~~~
### Create a Virtual Environment:
~~~
    python -m venv venv
~~~
### Activate the environment:
~~~
    # For PowerShell
    .\venv\Scripts\Activate.ps1
    # For Command Prompt
    .\venv\Scripts\activate.bat
~~~
### Install Dependencies:
~~~
    pip install -r requirements.txt
~~~
## Database Initialization
Before running the ingestion pipeline, you must initialize the database.
### Initialize the Schema:
~~~
    python setup_db.py
~~~