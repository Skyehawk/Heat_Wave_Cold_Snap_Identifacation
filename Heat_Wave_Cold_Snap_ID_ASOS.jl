#Heat_Wave_Cold_Snap_ID_ASOS.jl

using Base
using Dates
using Missings
using ShiftedArrays
using Statistics

using BenchmarkTools
using DataFrames
using DataFramesMeta
using CSV
using Glob

using RollingFunctions
#using RollingTimeWindows: RollingTimeWindow

import Pandas


function toPdDf(df)
  Pandas.DataFrame(DataFrame(df))
end

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

function load_ASOS(files)
	join_col= ""

	types_ = [fill(String,2); fill(Float64,12); fill(String,9); fill(Float64,7); fill(String,5)]
	select_ = ["station","valid","lon","lat","tmpf","dwpf", "skyc1", "skyc2", "skyc3", "skyc4"]
	missingstrings_ = ["","","","","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M","M",""]
	temp_dfs=Vector{DataFrame}(undef, length(files)) #Create a vector of empty dataframes.
	for i in 1:length(files)
	    temp_dfs[i]=CSV.read(files[i],DataFrame,ignorerepeated=false,delim=',',
	        skipto=7, header=6,types=types_, select=select_, missingstrings=missingstrings_,
	        silencewarnings=true) #Read each  infile to its own dataframe.
	end

	df = reduce(vcat, temp_dfs)
	colnames = ["STATION", "UTC_DATETIME_STR", "LON", "LAT", "AIR_TEMPERATURE_F", "AIR_DEW_PT_F", "SKYC1", "SKYC2", "SKYC3", "SKYC4"]
	rename!(df, colnames)
	return df
end

function load_USCRN(files)
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
	return df
end

function format_data(mode, df)
	if mode == "Temp"
		dropmissing!(df::AbstractDataFrame, "AIR_TEMPERATURE_F")
	elseif mode == "Sky_C"
		dropmissing!(df::AbstractDataFrame, "SKYC1")
	end


	date_format = DateFormat("yyyy-mm-dd HH:MM")
	df.UTC_DATETIME = DateTime.(df.UTC_DATETIME_STR, date_format)
	return df
end

function extract_runs(run_type, thresh, df)
	if run_type == "Temp"
		df.THRESH_ORDINAL_RANK = sign.(df.AIR_TEMPERATURE_F .- thresh)
		df.RUN_LENGTH = (df.THRESH_ORDINAL_RANK .- lag(df.THRESH_ORDINAL_RANK)).!= 0
		df.RUN_LENGTH[:1] = 0  #We set this to 0 due to the diff() (lag()) fuction 1 line above, we do not know what occured t=-1 records ago
		df.RUN_LENGTH = cumsum(convert(Array{Union{Missing, Int64}}, df.RUN_LENGTH)) #This should have no effect, however if there is a missing this will skip over it
	elseif run_type == "Sky_C"
		cover_dict = Dict("   "=>4.0, "SKC"=>4.0, "NCD"=>3.0, "CLR"=>3.0, "NSC"=>3.0, "FEW"=>2.0, "SCT"=>2.0, "BKN"=>1.0, "OVC"=>0.0, "VV "=>0.0, missing =>4.0)
		df.SKYC1 = getindex.(Ref(cover_dict), df.SKYC1)
		df.SKYC2 = getindex.(Ref(cover_dict), df.SKYC2)
		df.SKYC3 = getindex.(Ref(cover_dict), df.SKYC3)
		df.SKYC4 = getindex.(Ref(cover_dict), df.SKYC4)
		transform!(df, :, [:SKYC1,:SKYC2,:SKYC3,:SKYC4] => ByRow(min) => :SKYC)
		df.SKYC_MED_SMOOTH = runmedian(df.SKYC, 3)

		df.THRESH_ORDINAL_RANK = sign.(df.SKYC_MED_SMOOTH .- thresh)
		df.RUN_LENGTH = (df.THRESH_ORDINAL_RANK .- lag(df.THRESH_ORDINAL_RANK)).!= 0
		df.RUN_LENGTH[:1] = 0  #We set this to 0 due to the diff() (lag()) fuction 1 line above, we do not know what occured t=-1 records ago
		df.RUN_LENGTH = cumsum(convert(Array{Union{Missing, Int64}}, df.RUN_LENGTH)) #This should have no effect, however if there is a missing this will skip over it
	end

	gdf = groupby(df,:RUN_LENGTH)
	temp_dfs=Vector{DataFrame}(undef, length(gdf)) #Create a vector of empty dataframes.
	gdf[1]
	for i in 1:length(gdf)
	    dt_miss = first(gdf[i].UTC_DATETIME)
	    if i > 1
	        dt_miss = last(gdf[i-1].UTC_DATETIME)
	    end
	    
	    sample_time_deltas = Dates.value.(gdf[i].UTC_DATETIME .- 
	            lag(gdf[i].UTC_DATETIME, default = dt_miss))
	    
	    temp_dfs[i]= DataFrame(UTC_BEGIN_DATETIME = first(gdf[i].UTC_DATETIME),
	        UTC_END_DATETIME = last(gdf[i].UTC_DATETIME),
	        UTC_MAX_TIMESTEP = maximum(sample_time_deltas),
	        UTC_MEDIAN_TIMESTEP = median(sample_time_deltas),
	        UTC_DURATION = last(gdf[i].UTC_DATETIME)- first(gdf[i].UTC_DATETIME),
	        THRESH_ORDINAL_RANK = first(gdf[i].THRESH_ORDINAL_RANK),
	        MEAN_TEMP = mean(gdf[i].AIR_TEMPERATURE_F),
	        MAX_TEMP = maximum(gdf[i].AIR_TEMPERATURE_F),
	        MIN_TEMP = minimum(gdf[i].AIR_TEMPERATURE_F),
	        DEGREE_MINS = sum(-(gdf[i].AIR_TEMPERATURE_F .- thresh) .*
	            (sample_time_deltas /60000)),
	        N_RECORDS = length(gdf[i].AIR_TEMPERATURE_F))#,
	        #SKYC = mean(gdf[i].SKYC),
	        #SKYC_MED_SMOOTH = mean(gdf[i].SKYC_MED_SMOOTH))
	end

	res = reduce(vcat, temp_dfs)
	return res
end

function temporal_filter(min_duration, thresh_dir, res, show_flag)
	min_duration = Minute(min_duration)

	if thresh_dir == "below"
		res = sort(res[res[:,:THRESH_ORDINAL_RANK] .== -1, :], [:UTC_DURATION, :MEAN_TEMP], rev=true)
	elseif thresh_dir == "above"
		res = sort(res[res[:,:THRESH_ORDINAL_RANK] .== 1, :], [:UTC_DURATION, :MEAN_TEMP], rev=true)
	end
	
	res = sort(res[res[:,:UTC_DURATION] .>= min_duration, :], [:UTC_DURATION, :MEAN_TEMP], rev=true)

	# this commented out (privides times in Days, Mins, Seconds), as passing to python pandas does not have an equivlent, may be able to pass this as a string, but we can pass the millisecond values and convert it on the front end with python
	#transform!(res, :UTC_DURATION => ByRow((dt)-> Dates.canonicalize(Dates.CompoundPeriod(dt))))
	if show_flag
		show(first(res, 15), allcols=true)
	end
	return toPdDf(res)
end
