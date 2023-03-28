# aperc-template
Template for APERC models.

## How to use this template
Create a new repository. When given the option, select 'aperc-template' as the template.

## Project organization

Project organization is based on ideas from [_Good Enough Practices for Scientific Computing_](https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1005510) and the [_SnakeMake_](https://snakemake.readthedocs.io/en/stable/snakefiles/deployment.html) recommended workflow. 

1. Put each project in its own directory, which is named after the project.
2. Put data in the `data` directory. This can be input data or data files created by scripts and notebooks in this project.
3. Put configuration files in the `config` directory.
4. Put text documents associated with the project in the `docs` directory.
5. Put all scripts in the `workflow/scripts` directory.
6. Install the Conda environment into the `workflow/envs` directory. 
7. Put all notebooks in the `workflow/notebooks` directory.
8. Put final results in the `results` directory.
9. Name all files to reflect their content or function.

## Using Conda

### Creating the Conda environment

After adding any necessary dependencies to the Conda `environment.yml` file you can create the 
environment in a sub-directory of your project directory by running the following command.

```bash
$ conda env create --prefix ./env --file ./workflow/environment.yml
```
Once the new environment has been created you can activate the environment with the following 
command.

```bash
$ conda activate ./env
```

Note that the `env` directory is *not* under version control as it can always be re-created from 
the `environment.yml` file as necessary.

### Updating the Conda environment

If you add (remove) dependencies to (from) the `environment.yml` file after the environment has 
already been created, then you can update the environment with the following command.

```bash
$ conda env update --prefix ./env --file ./workflow/environment.yml --prune
```

### Listing the full contents of the Conda environment

The list of explicit dependencies for the project are listed in the `environment.yml` file. To see the full list of packages installed into the environment run the following command.

```bash
conda list --prefix ./env
```

