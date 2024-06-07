## Energy balance tables (EBT) system for the APERC outlook

This system is generally used for (and in this order):

1. taking in Energy balance tables from the ESTO team and transforming that into the fuel/sector categories to be used in the Outlook by the modellers.
2. creating a layout file which the modellers can fill in with their data. this will contain the esto data for years previous to what the modellers are working on, and then be filled in with the modellers data for the years and secotr/fuels categories they are working on.
3. taking in the filled in layout files from each modeller and merging them all together. this is done a few times throughout each economy modelling process because (at least for the 9th workflow) first the demand modelling is done, then the merged file is used to inform the transformation modelling, then again for the supply modelling, and then the merged file is sent to the visualisation system to create the visualisations.
4. integrating non energy data into datasets based on the type. For example, capacity, cost and emissions data are important for the modelling process, but are not included in the energy balance tables. 

This system is written with the intention to be used for subsequent Outlooks, not jsut the ninth, as long as it remains helpful. For that reason a lot of effort has been made to make it robust and flexible. Some example of this are: (todo: make step 1 and 2 above more robust and flexible), step 3 contains a lot of checks to make sure the data is in the correct format and the values the modellers are reporting are as expected. 

#### Step 3: merging the modellers data
This step is a bit more complicated than we expected when we started. 

One big reason is because we realsied that we needed to be able to idenitfy subtotals within the data and label them. This was partly because we had shifted the visualisation system to rely on the data being in a consistent structure so that the same code, queries, aggregations and transformation could be used for each economy without worrying about accidentally including a row which was actually a subtotal, and theforre double counting.
The process of identifying and labelling subtotals invovles many checks, because there are many pitfalls to avoid, due to the inherent messiness of balances tables. Efforts should be made to ensure these checks are always clear and easy to understand, so that they can be easily updated if necessary.
Labelled subtotals are also a useful thing to have in the data for the modellers and researchers to use, so that they dont have to worry about double counting.
There are also some checks to make sure that values sum to correct totals and are in the correct format.

#### Step 4: integrating non energy data
This data is integrated or, in the case of emissions, calculated separately and in a more simplified but less flexible way than the energy data. This is partly because this data is not needed to be passed between modellers, and partly because it is not as messy (no subtotals) as the energy balance tables, so it is easier to integrate it into the system in a more straightforward way.
The onus is on the modellers to ensure that their data is in the correct format, and that they are reporting the correct values. 

### To do List:
- consider whether we want 19_total fuel and also subtotals of fuels within 09_total_transformation. The creation of these totals creates confusing values since they are the sums of negatives (input_fuel) and positives (output_fuel), e.g. -natural_gas + lng
  
  
### Incorporating supply components repo into the EBT system:

*Note that the supply components repo also contains scripts to project pipeline transport demand and transformation own use. But from hereon they will be referred to by their purpose or as 'supply components'. The supply component of the supply components repo will have it's purpose referred to as 'minor supply components'.*

For the short term we have decided to include the supply components repo in the EBT system. This is because the supply components are simple scripts that dont need to be changed much, and it is easier to keep them in the same system as the EBTs so they dont need to be run manually. The code has been designed to be easily run separately using the output from the EBT system, so it is easy to separate them again if necessary. Some slight changes to the supply components system methodology were needed, although these changes only involved being more specific about the data that was used for calculations.

These components will be run after the merging_results() function, to simulate the process of runnning the EBT system, giving the merged results to the modellers and then having the modeller run the supply components using those merged results (if the merged results dont have the necessary data the supply components will simply just calculate values using 0's). *note that this means that its important the EBT operator has all the correct data (e.g. all demand data for pipelines and trans_own_use_addon) in the data\modelled_data\ECONOMY_ID folder before running the supply components functions, because they will not get notified if this isnt the case.*

At the end of running the supply components functions, the results will be saved into the data/modelled_data folder, simulating the modeller running the supply components process, saving the results into the integration folder and the EBT system operator then taking those results and putting them in the modelled_data folder. In the case of pipeline transport and transformation own use, the EBT operator would've needed to run the EBT as soon as this data is put into integration, however this process will also just run the merging_results() function again after running the supply components functions to automatically update the data. This makes the supply components functions very unobtrusive, even shortening the previous process!

The supply components contains 3 scripts: 

1. pipeline transport: 
This takes in data from the EBT system and separates historical data from projection data. It then calculates the ratio of gas consumption to total consumption in the pipeline sector and uses this ratio to calculate the energy consumption (of gas, petroleum prods and electricity) in the pipeline sector for the projection years.  This was done after demand was modelled, because the energy used for pipeline transport is a function of the demand of gas.

2. transformation, own use and nonspecified (trans_own_use_addon()):
Much like the pipeline_transport function, this function takes in demand data from the EBT system and separates historical data from projection data. It then calculates the energy consumption in the calculates the energy used for other-transformation, own use and nonspecified for the projection years.  This is also only done after demand is modelled. 

3. minor supply components:
this script takes in the transformation and demand data and calculates the energy used for some minor supply components (e.g. biofuel supply). This is done after all transformation is modelled, at the same time as the supply modelling is done. *this could cause minor confusion for supply or transformation modellers if they accidentally think they need to use any of the outputs from this in their modelling. Although this doesnt seem likely, it is something to be aware of.*

## Using Conda

### Creating the Conda environment

After adding any necessary dependencies to the Conda `environment.yml` file you can create the 
environment in a sub-directory of your project directory by running the following command.

```bash
$ conda env create --prefix ./env --file ./workflow/envs/environment.yml
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
