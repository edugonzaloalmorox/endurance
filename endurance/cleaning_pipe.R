
library(tidyverse)

bikes_2023 = read_csv('data/processed/df_2023_complete.csv')


# ------------------------------
# Cleaning variables and data
# -----------------------------

# Polish race  names -------- 
races = bikes_2023 %>%
  count(race)

# ------------------------------
# Cleaning variables and data
# -----------------------------

# Polish race  names -------- 
races = bikes_2023 %>%
  count(race)


races = races %>%
  mutate(race_clean = str_remove(race, '2023'),
         race_clean = paste0(race_clean, " 2023")) %>%
  mutate(race_clean = case_when(
    race_clean == "asajaun  2023" ~ "Basajaun 2023",
    race_clean == "hemian Border Bash Race  2023" ~ "Bohemian Border Bash Race  2023",
    race_clean == "laenau 600  2023" ~ "Blaenau 600  2023",
    race_clean == "right Midnight  2023" ~ "Bright Midnight  2023",
    race_clean == "the Capitals by PedAlma  2023" ~ "The Capitals by PedAlma  2023",
    race_clean == "the TransAtlantic Way 2023" ~ "The TransAtlantic Way 2023",
    
    TRUE ~ race_clean
  ), 
  race_clean = str_trim(race_clean, 'both')) 


off_road = c('Arizona Trail Race (AZTR) 2023','Arkansas High Country Race 2023',
              'Ascend Armenia + Event Backstory 2023',
              "Atlas Mountain Race 2023",
              "Colorado Trail Race 2023",
              "Hellenic Mountain Race 2023",
              "Highland Trail 550 (HT550) 2023",
              "Iditarod Trail Invitational 2023",
              "Silk Road Mountain Race (SRMR) 2023",
              "Stagecoach 400 2023",
              "Tour Divide (Part 1): Drop-Bar Bike 2023",
              "Tour Divide (Part 2): Flat-Bar Bike 2023",
              "Trans Balkan Race 2023",
              "Log Driver\'s Waltz  2023",
              "Mountains of the Merfynion  2023",
              "Of The Great British Divide  2023", 
              "Trans Balkans Race  2023", 
              "Blaenau 600  2023")


gravel = c("Badland 2023", 
             "Pinyons and Pine 2023",
             "Across Andes  2023",
             "At Last Lost  2023", 
             "DEAD ENDS & dolci  2023",
             "Dorset Divide  2023", 
             "GranGuanche Audax Gravel  2023",
             "Gravel Birds  2023",
             "Highland Trail 550  2023",
             "Hope 1000  2023", 
             "Istra Land  2023",
             "Of All Points North  2023",
             "Of Great British Escapades  2023", 
             "Of The Southern Divide - Spring Edition  2023", 
             "Quick Bite!  2023", 
             "Race Around the Netherlands GX  2023",
             "Race around Rwanda  2023",
             "Seven Serpents  2023", 
             "Taunus Bikepacking No. 6 2023",
             "The Southern Divide - Autumn Edition  2023",
             "Unmapping: Sweden  2023",
             "Basajaun 2023",
             "Bohemian Border Bash Race  2023",
             "Bright Midnight  2023")

road = c("Kromvojoj  2023",
           "Of Transpyrenees by Transibérica  2023",
           "Of Madrid To Barcelona by PedAlma  2023",
           "Race Through Poland  2023",
           "Trans Pyrenees Race  2023",
           "Transibérica  2023",
           "Mittelgebirge Classique  2023",
           "Of B-HARD Ultra Race and Brevet  2023",
           "Of The Perfidious Albion  2023",
           "Of Utrecht Ultra  2023", 
           "Pan Celtic Race  2023",
           "Solstice Sprint  2023",
           "The Ardennes Monster  2023",
           "The Unknown Race  2023",
           "The Wild West Country  2023",
           "Three Peaks Bike Race  2023",
           "The Capitals by PedAlma  2023",
           "the TransAtlantic Way  2023")


races = races %>%
   mutate(type = case_when(
     race_clean %in% gravel ~ 'gravel',
     race_clean %in% off_road ~ 'off_road',
     race_clean %in% road ~ 'road',
     TRUE ~ NA
 
   ))

race_dict = races %>%
  select(race, race_clean, type)

bikes_2023 = bikes_2023 %>%
  left_join(race_dict, by = 'race')

# Polish brand names --------------------- 

brands = bikes_2023 %>%
  count(bike_brand) 


brands_dict = brands %>%
  mutate(bike_clean = str_to_title(str_to_lower(bike_brand))) %>%
  arrange(bike_clean) %>%
  ungroup %>%
  mutate(grouped_brand = case_when(
      str_to_lower(bike_clean) %in% c("specialised", "specialized", "specializer", "spezialized") ~ "Specialized",
      str_to_lower(bike_clean) %in% c("s-works", "sworks") ~ "S-Works",
      str_to_lower(bike_clean) %in% c("cervelo", "cervelélo", "cervélo", "cérvelo") ~ "Cervelo",
      str_to_lower(bike_clean) %in% c("9zero7") ~ "9:Zero:7",
      str_to_lower(bike_clean) %in% c("all city", "all city cycles", "all-city") ~ "All City",
      str_to_lower(bike_clean) %in% c("allied") ~ "Allied Cycle Works",
      str_to_lower(bike_clean) %in% c("binary", "binary bicycle") ~ "Binary Bicycles",
      str_to_lower(bike_clean) %in% c("blacksheep") ~ "Black Sheep",
      str_to_lower(bike_clean) %in% c("brother") ~ "Brother Cycles",
      str_to_lower(bike_clean) %in% c("cayon") ~ "Canyon",
      str_to_lower(bike_clean) %in% c("custom marino", "custom selfmade", "custom ti frame", 
                                      "custom titanium", "custom built torus titanium",
                                      'self build bamboo') ~ "Custom",
    
      str_to_lower(bike_clean) %in% c("cutthroat") ~ "Salsa",
      str_to_lower(bike_clean) %in% c("b'twin", "rockrider", "triban", "van rysel") ~ "Decathlon", 
      str_to_lower(bike_clean) %in% c("fair light", 'fairlght') ~ "Fairlight",
      str_to_lower(bike_clean) %in% c("fara cycling") ~ "Fara", 
      str_to_lower(bike_clean) %in% c("j. guillem", "j.guillem", "jguillem") ~ "J Guillem",
      str_to_lower(bike_clean) %in% c("j laverack", "j.laverack") ~ "J. Laverack",
      str_to_lower(bike_clean) %in% c("fiftyone bikes") ~ "Fiftyone",
      str_to_lower(bike_clean) %in% c("genesis uk") ~ "Genesis",
      str_to_lower(bike_clean) %in% c("jaeger") ~ "Jaegher",
      str_to_lower(bike_clean) %in% c("jerónimo sfarrapa") ~ "Jeronimo",
      str_to_lower(bike_clean) %in% c("custom built by jesse turner of slow southern steel") ~ "Jesse Turner",
      str_to_lower(bike_clean) %in% c("lappiere") ~ "Lapierre",
      str_to_lower(bike_clean) %in% c("lauf cycling") ~ "Lauf",
      str_to_lower(bike_clean) %in% c("lynsky") ~ "Lynskey",
      str_to_lower(bike_clean) %in% c("mason") ~ "Mason Cycles",
      str_to_lower(bike_clean) %in% c("nordest") ~ "Nørdest",
      str_to_lower(bike_clean) %in% c("on-one") ~ "On One",
      str_to_lower(bike_clean) %in% c("open", "open min.d.", "open one+", 
                                      "open u.p", "open u.p.", "open up", "opencycle") ~ "Open",
      str_to_lower(bike_clean) %in% c("panorama") ~ "Panorama Cycles",
      str_to_lower(bike_clean) %in% c("planetx") ~ "Planet X",
      str_to_lower(bike_clean) %in% c("pipedream") ~ "Pipedream Cycles",
      str_to_lower(bike_clean) %in% c("revel") ~ "Revel Bikes",
      str_to_lower(bike_clean) %in% c("rodeo adventure labs", "rodeolabs") ~ "Rodeo Labs",
      str_to_lower(bike_clean) %in% c("rose") ~ "Rose Bikes",
      str_to_lower(bike_clean) %in% c("scot", 'scott quiring') ~ "Rodeo Labs",
      str_to_lower(bike_clean) %in% c("seven") ~ "Seven Cycles",
      str_to_lower(bike_clean) %in% c("solace", 'ti solace cycles') ~ "Solace Cycles",
      str_to_lower(bike_clean) %in% c("sour", "sour pasta party") ~ "Sour Bikes",
      str_to_lower(bike_clean) %in% c("stayer") ~ "Stayer Cycles",
      str_to_lower(bike_clean) %in% c("why") ~ "Why Cycles", 
      str_to_lower(bike_clean) %in% c("wilier") ~ "Wilier Triestina",
      str_to_lower(bike_clean) %in% c("windover") ~ "Windower Bikes",
      str_to_lower(bike_clean) %in% c("yt") ~ "Yt industries",
      str_to_lower(bike_clean) %in% c("wi.de") ~ "Wi-De",
      TRUE ~ bike_clean  # Keep the original name if it doesn't match any conditions
))  %>%
  select(bike_brand, grouped_brand) %>%
  unique()
  
# Link information ----------
bikes_2023 = bikes_2023 %>%
  left_join(brands_dict, by = "bike_brand") 



app_df = bikes_2023 %>%
  select(name:country, race_clean, grouped_brand, bike_models, type) %>%
  rename(race = race_clean, 
         bike_brand = grouped_brand,
         type_race = type)

write_csv(app_df, 'data/processed/app_df.csv')




    