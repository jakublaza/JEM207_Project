#### Obtaining data for map of districs of the Czech Republic, given that Prague is officially not a district, but a region. We download data for Prague from region database and add them to districts
#### Hence we treat Prague as a district. 
# %%
import geopandas
link_dist = "/Users/Jakub/Downloads/BND/PolbndDistDA.shp"
districts = geopandas.read_file(link_dist)

link_reg = "/Users/Jakub/Downloads/BND/PolbndRegDA.shp"
regions = geopandas.read_file(link_reg)

prague = regions.iloc[5]

#### We preforme some adjustments so the merge with covid data is smooth. We will be merging on column "district_code" 

prague = prague.drop(labels = ["REG_LABEL", "ANND"]).rename({"SHN1":"district_code"})

districts = districts.rename(columns = {"SHN2":"district_code"})

districts = districts.append(prague).drop(columns = ["NAMA", "DESN", "ISN", "F_CODE", "FCSUBTYPE"]).set_index("NAMN")

for i in range(1, 78):
    districts["district_code"][i] = districts["district_code"][i][:6]

districts.plot()