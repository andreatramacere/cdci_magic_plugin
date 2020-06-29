MAGIC cdci plugin
==========================================
*MAGIC plugin for cdci_data_analysis*


What's the license?
-------------------

magic cdci plugin is distributed under the terms of The MIT License.

Who's responsible?
-------------------
Andrea Tramacere

ISDC Data Centre for Astrophysics, Astronomy Department of the University of Geneva, Chemin d'Ecogia 16, CH-1290 Versoix, Switzerland

Configuration for deployment
----------------------------
- copy the conf_file from 'cdci_magic_plugin/config_dir/data_server_conf.yml' and place in given directory
- set the env var `CDCI_MAGIC_PLUGIN_CONF_FILE` to the path of the file conf_file 
- edit the in conf_file the two keys:
    - `data_server_url:`  
    - `data_server_port:`
    
    these two keys must correspond to those in the magic-backend conf_file ie:
   
    - `data_server_url:`  -> `url:`
    
    - `data_server_port:` ->`port:`
   
    respectively