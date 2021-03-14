using Base
using Dates
using Missings
using ShiftedArrays

using BenchmarkTools
using DataFrames
@using DataFramesMeta
using CSV
using Glob

function find_files(path, pattern, ext)
    if isempty(pattern)
        files=glob("**/*"*ext, path) #Vector of filenames. Glob allows you to use the asterisk.
        println("Number of files: ", length(files))    #Number of files to read.
        return files
    elseif !isempty(pattern)
        files=glob("**/"*pattern*ext, path) #Vector of filenames. Glob allows you to use the asterisk.
        println("Number of files: ", length(files))    #Number of files to read.
        return files
    end
    return []
end

parent_dir = "D:/Libraries/Documents/Climatology_Personal/USCRN_USRCRN_SUBHOURLY/"
pattern = "*-MN_Good*"

files = find_files(parent_dir,pattern,".txt")
join_col= ""
header_ = ["WBANNO", "UTC_DATE", "UTC_TIME", "LST_DATE", "LST_TIME", "CRX_VN", "LONGITUDE",
    "LATITUDE", "AIR_TEMPERATURE", "PRECIPITATION", "SOLAR_RADIATION", "SR_FLAG",
    "SURFACE_TEMPERATURE", "ST_TYPE", "ST_FLAG", "RELATIVE_HUMIDITY", "RH_FLAG",
    "SOIL_MOISTURE_5", "SOIL_TEMPERATURE_5", "WETNESS WET_FLAG", "WIND_1_5", "WIND_FLAG"]
types_ = [Int; fill(String, 5); fill(Float64, 5); String; Float64; fill(String,2);
    Float64; String; fill(Float64,3); String; Float64; String]
select_ = ["WBANNO","UTC_DATE","UTC_TIME","LST_DATE","LST_TIME","LONGITUDE","LATITUDE",
    "AIR_TEMPERATURE"]
missingstrings_ = ["","","","","","","","-9999.0","","","","-9999.0","","","","","","-9999.0","","",""]
temp_dfs=Vector{DataFrame}(undef, length(files)) #Create a vector of empty dataframes.
for i in 1:length(files)
    temp_dfs[i]=CSV.read(files[i],DataFrame,ignorerepeated=true,delim=' ',
        header=header_,types=types_, select=select_, missingstrings=missingstrings_,
        silencewarnings=true) #Read each  infileto its own dataframe.
end

df = reduce(vcat, temp_dfs)
describe(df)

dropmissing!(df::AbstractDataFrame, "AIR_TEMPERATURE")

date_format = DateFormat("yyyymmdd HHMM")
df.UTC_DATETIME = DateTime.(df.UTC_DATE.*" ".*df.UTC_TIME,date_format)
df.LST_DATETIME = DateTime.(df.LST_DATE.*" ".*df.LST_TIME,date_format)
select!(df, Not([:UTC_DATE, :UTC_TIME, :LST_DATE, :LST_TIME]))

for (x,y) in enumerate(zip(eltype.(eachcol(df)),names(df)))
    @show x, y
    
end

temp_thresh = -30.
df.THRESH_ORDINAL_RANK = sign.(df.AIR_TEMPERATURE.-temp_thresh)
df.RUN_LENGTH = (df.THRESH_ORDINAL_RANK .- lag(df.THRESH_ORDINAL_RANK)).!= 0
df.RUN_LENGTH[:1] = 0  #We set this to 0 due to the diff() fuction 1 line above, we do not know what occured t=-1 records ago
df.RUN_LENGTH = cumsum(convert(Array{Union{Missing, Int64}}, df.RUN_LENGTH))

gdf = groupby(df,:RUN_LENGTH)
temp_dfs=Vector{DataFrame}(undef, length(gdf)) #Create a vector of empty dataframes.
gdf[1]
for i in 1:length(gdf)  
    temp_dfs[i]= DataFrame(UTC_BEGIN_DATETIME = first(gdf[i].UTC_DATETIME),
        UTC_END_DATETIME = last(gdf[i].UTC_DATETIME),
        UTC_DURATION = last(gdf[i].UTC_DATETIME)- first(gdf[i].LST_DATETIME),
        LST_BEGIN_DATETIME = first(gdf[i].LST_DATETIME),
        LST_END_DATETIME = last(gdf[i].LST_DATETIME),
        THRESH_ORDINAL_RANK = first(gdf[i].THRESH_ORDINAL_RANK),
        MEAN_TEMP = mean(gdf[i].AIR_TEMPERATURE),
        MAX_TEMP = maximum(gdf[i].AIR_TEMPERATURE),
        MIN_TEMP = minimum(gdf[i].AIR_TEMPERATURE)

    )
end

res = reduce(vcat, temp_dfs)

show(sort!(res[res[:,:THRESH_ORDINAL_RANK] .== -1, :], [:UTC_DURATION, :MEAN_TEMP]), true)