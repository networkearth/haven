# Haven

## What is Haven?

Haven is designed to make database management accessible and efficient for researchers. As datasets grow, databases become essential for handling large volumes of data, providing the structure needed for efficient data operations and easy sharing with others. However, setting up and managing a database can be daunting, especially for researchers who are pressed for time and working with limited budgets.

Haven simplifies this process by offering a quick, cost-effective way to set up databases using AWS’s S3 and Athena platforms. With just a few steps—choosing a name, pointing to your AWS account, and initiating the build—Haven takes care of the rest, allowing you to create a fully operational database with minimal effort.

Once your database is established, Haven provides the tools to seamlessly read from and write to your data, ensuring that you can focus on your research without getting bogged down by the complexities of database management.

## Installing Haven

```
git clone git@github.com:networkearth/haven.git
cd haven
pip install .
```

## Setting up Your Database

Setting up your database is easy as pie! 

1. Configure the [AWS CLI](https://aws.amazon.com/cli/) to act as a user with cloudformation access. (It's best if you work with an account admin)
2. Ensure you have a recent enough version of `node` - `nvm install 22`
3. Fill out the configuration file `config.yaml`:

```yaml
database: my_database
account: '575101084097'
region: 'us-east-1'
```

4. Run `haven init config.yaml`

In addition to setting up your directory haven will build a directory with the name of your database that includes all of the cloud formation configuration and templates that were used to build your database in case you want to check on anything. 


