1. Reading multiple file types using generic pySpark module
- Creating multiple config files
- Creating multipule data sources
- generic pySpark program 


how are we doing config driven program



psudo code
-import and session creatation
-read file config.json 
-code for injusting 3 file types
-create df




def load_config(file_path):

    
    with open(file_path, 'r') as f:
        return json.load(f)





-----------------------------------------------

- send config file as argument while compiling program.
    - python3 main.py config1 ( learn about args )

- config {
    infer_Schema : ""
    schema  : ""
    infer_header : ""
    header : ""
}

--------------------------------------------------
