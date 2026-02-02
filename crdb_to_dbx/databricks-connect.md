
select a cluster
    Serverless
Python Environment
    Activate an environment with Python 3.11 (Python version will change depending on the cluster)
        Create a new environment
            venv
                Select Python 3.11.x (need to match the cluster)
                .venv_3.11.3
                install project dependencies
    Install Databricks-connect
        Click "Install" to the prompt
    Setup Databricks builtins for autocompletion
        Click "Continue"

create a notebook
    select the kernel
        click yes to install the kernel

    ```
    print(spark)
    ```

    ```
    print(dbutils)
    ```    
