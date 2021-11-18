
# Face The Facts

[Face The Facts](https://facethefacts.app/) is an open-source project that develops a mobile app to show politicians' information by scanning their election posters with a smartphone. Our mission is to make politicians' information (e.g., their past voting behaviours and CVs) more accessible and to encourage users to vote critically.
In our project, we focus on the German election system. With our architecture, you can build a Face The Facts app for a different country.

**Caution!!!**  
**This repository is for old setups for a database. Please check the src/db directory on our [backend](https://github.com/FaceTheFacts/backend) repository for the latest updates.**
## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`DATABASE_HOST`

`DATABASE_USER`

`DATABASE_PASSWORD`

`DATABASE_URL`
## Installation
You'll need to configure the virtual environment and install dependencies.
Ideally, Python 3.9.7:

```bash
  python3 -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
```
    
## Database
Our team utilises [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/) as a database and [SQLAlchemy](https://www.sqlalchemy.org/) as Object Relational Mapper and defines database connection in the src/db/connection.py.

Inside the src/models, our team defines data models. We create a migration environment with [Alembic](https://alembic.sqlalchemy.org/en/latest/). Our Entity-Relationship diagram is as follows.
![FTF_ERD](https://user-images.githubusercontent.com/78789212/142422328-3a72fcea-0388-495f-b7b7-4d8c78faabca.png)



## Branch Naming Convention
If you create a branch for features, you should name it like:
`feature/[description]`

If you create a branch for bugs, you should name it like:
`bug/[description]`
