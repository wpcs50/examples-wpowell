# tdm23

Python Environment setup

*  Make sure the anaconda is installed
* Open anaconda prompt and type `conda env list` to check the existing environment
* If tdm environment is not installed, type `conda env create -f   ` "path_to_env/*.YML"
* If an environment is already installed, to update it type `conda env update --name env_name --file path_to_env/*.YML --prune`
* Once the environment is created, run  `conda env list` to check the location of the new environment
* Replace the  `condaBat` variable in the scenario file using the new location
  * or change the parameters in TransCAD   

