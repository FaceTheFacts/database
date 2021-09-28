
# Face The Facts (Database)




## Branch Naming Convention
If you create a branch for features, you should name it like:
`feature/[description]`
If you create a branch for bugs, you should name it like:
`bug/[description]`

## Deployment

You'll need to configure the virtual environment and install dependencies.
Ideally, Python 3.9.7:

```bash
  python3 -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
```

  
## Environment Variables

To run this project, you will need to add the following environment variables to your .env file

`DATABASE_HOST`

`DATABASE_USER`

`DATABASE_PASSWORD`
