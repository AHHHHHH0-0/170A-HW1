use newDatabaseName;

# acm_fellows

# DROP TABLE acm_fellows;

CREATE TABLE acm_fellows(  
    name VARCHAR(255),
    year int
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/acm-fellows.csv'
INTO TABLE acm_fellows
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# conference_ranking

# DROP TABLE conference_ranking;

CREATE TABLE conference_ranking(  
    abbreviation VARCHAR(255),
    conference_name VARCHAR(255),
    ranking VARCHAR(255)
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/conference_ranking.csv'
INTO TABLE conference_ranking
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE conference_ranking
ADD COLUMN academic_society VARCHAR(255);

UPDATE conference_ranking
SET academic_society = CASE 
    WHEN INSTR(conference_name, 'ACM') > 0 AND INSTR(conference_name, 'IEEE') > 0 THEN 'ACM+IEEE'
    WHEN INSTR(conference_name, 'ACM') > 0 THEN 'ACM'
    WHEN INSTR(conference_name, 'IEEE') > 0 THEN 'IEEE'
    ELSE 'other'
END;

# country_info

# DROP TABLE country_info;

CREATE TABLE country_info(  
    institution VARCHAR(255),
    region VARCHAR(255),
    countryabbrv VARCHAR(255)
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/country-info.csv'
INTO TABLE country_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# clean_csrankings (hw2)

DROP TABLE clean_csrankings;
# DROP TABLE partial_csrankings;
# DROP TABLE cs_rankings;

CREATE TABLE clean_csrankings(  
    clean_name VARCHAR(255),
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    surname VARCHAR(255),
    affiliation VARCHAR(255),
    homepage VARCHAR(400),
    scholar VARCHAR(255),
    middle_name VARCHAR(255)
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/clean_csrankings.csv'
INTO TABLE clean_csrankings
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# institution

# DROP TABLE institution;

CREATE TABLE institution(  
    institution VARCHAR(255),
    school_type VARCHAR(255),
    alias VARCHAR(255),
    state_name VARCHAR(255),
    city VARCHAR(255),
    zip VARCHAR(20),
    region VARCHAR(255),
    isPublic VARCHAR(10),
    institutionalControl VARCHAR(255),
    primaryPhotoCardThumb VARCHAR(400),
    displayRank VARCHAR(50),
    sortRank INT,
    isTied VARCHAR(10),
    actAvg_rawValue FLOAT,
    percentReceivingAid_rawValue FLOAT,
    acceptanceRate_rawValue FLOAT,
    tuition_rawValue FLOAT,
    hsGpaAvg_rawValue FLOAT,
    engineeringRepScore_rawValue VARCHAR(50),
    parentRank_rawValue FLOAT,
    enrollment_rawValue INT,
    businessRepScore_rawValue VARCHAR(50),
    satAvg_rawValue FLOAT,
    costAfterAid_rawValue FLOAT,
    testAvgs_displayValue_1_value VARCHAR(50),
    testAvgs_displayValue_2_value VARCHAR(50)
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/data.csv'
INTO TABLE institution
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@institution, @school_type, @alias, @state_name, @city, @zip, @region, @isPublic, @institutionalControl, @primaryPhotoCardThumb, @displayRank, sortRank, isTied, @actAvg_rawValue, @percentReceivingAid_rawValue, @acceptanceRate_rawValue, @tuition_rawValue, @hsGpaAvg_rawValue, @engineeringRepScore_rawValue, @parentRank_rawValue, @enrollment_rawValue, @businessRepScore_rawValue, @satAvg_rawValue, @costAfterAid_rawValue, @testAvgs_displayValue_1_value, @testAvgs_displayValue_2_value)
SET
    institution = NULLIF(@institution, ''),
    school_type = NULLIF(@school_type, ''),
    alias = NULLIF(@alias, ''),
    city = NULLIF(@city, ''),
    state_name = NULLIF(@state_name, ''),
    zip = NULLIF(@zip, ''),
    region = NULLIF(@region, ''),
    isPublic = NULLIF(@isPublic, ''),
    institutionalControl = NULLIF(@institutionalControl, ''),
    primaryPhotoCardThumb = NULLIF(@primaryPhotoCardThumb, ''),
    displayRank = NULLIF(@displayRank, ''),
    actAvg_rawValue = NULLIF(@actAvg_rawValue, ''),
    percentReceivingAid_rawValue = NULLIF(@percentReceivingAid_rawValue, ''),
    acceptanceRate_rawValue = NULLIF(@acceptanceRate_rawValue, ''),
    tuition_rawValue = NULLIF(@tuition_rawValue, ''),
    hsGpaAvg_rawValue = NULLIF(@hsGpaAvg_rawValue, ''),
    engineeringRepScore_rawValue = NULLIF(@engineeringRepScore_rawValue, ''),
    parentRank_rawValue = NULLIF(@parentRank_rawValue, ''),
    enrollment_rawValue = NULLIF(@enrollment_rawValue, ''),
    businessRepScore_rawValue = NULLIF(@businessRepScore_rawValue, ''),
    satAvg_rawValue = NULLIF(@satAvg_rawValue, ''),
    costAfterAid_rawValue = NULLIF(@costAfterAid_rawValue, ''),
    testAvgs_displayValue_1_value = NULLIF(@testAvgs_displayValue_1_value, 'N/A'),
    testAvgs_displayValue_2_value = CASE 
        WHEN LOCATE(@testAvgs_displayValue_1_value, 'N/A') > 0 then NULL
        ELSE @testAvgs_displayValue_2_value
        END;

# dblp_aliases

# DROP TABLE dblp_aliases;

CREATE TABLE dblp_aliases(  
    alias VARCHAR(255),
    name VARCHAR(255)
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/dblp-aliases.csv'
INTO TABLE dblp_aliases
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(alias, @state_name)
SET
    name = NULLIF(@state_name, '');

ALTER TABLE dblp_aliases
ADD COLUMN clean_author_name VARCHAR(255)

UPDATE dblp_aliases
SET clean_author_name = CASE 
    WHEN RIGHT(name, 4) REGEXP '^-?[0-9]+$' THEN LEFT(name, LENGTH(name) - 5)
    ELSE name
END;

# field_conference

# DROP TABLE field_conference;

CREATE TABLE field_conference(  
major VARCHAR(255),
field_name VARCHAR(255),
conference VARCHAR(255)
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/field_conference.csv'
INTO TABLE field_conference
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# generated_author_info

# DROP TABLE partial_generated_author_info;

CREATE TABLE partial_generated_author_info(  
    name VARCHAR(255),
    department VARCHAR(255),
    area_name VARCHAR(400),
    count_value FLOAT,
    adjusted_count FLOAT,
    year_value int
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/generated-author-info.csv'
INTO TABLE partial_generated_author_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE partial_generated_author_info
ADD COLUMN clean_author_name VARCHAR(255)

UPDATE partial_generated_author_info
SET clean_author_name = CASE 
    WHEN RIGHT(name, 4) REGEXP '^-?[0-9]+$' THEN LEFT(name, LENGTH(name) - 5)
    ELSE name
END;

# DROP TABLE generated_author_info;

CREATE TABLE generated_author_info (
    clean_name VARCHAR(255),
    full_name VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    department VARCHAR(255),
    area_name VARCHAR(400),
    count_value FLOAT,
    adjusted_count FLOAT,
    year_value int);

INSERT INTO generated_author_info (clean_name, full_name, first_name, last_name, area_name, department, count_value, adjusted_count, year_value)     
SELECT clean_author_name, name, SUBSTRING_INDEX(clean_author_name, ' ', 1),  SUBSTRING_INDEX(clean_author_name, ' ', -1), area_name, department, count_value, adjusted_count, year_value 
FROM partial_generated_author_info;

ALTER TABLE generated_author_info
ADD COLUMN middle_name VARCHAR(255);

UPDATE generated_author_info
SET middle_name = CASE 
    WHEN CHAR_LENGTH(REPLACE(clean_name, ' ', '')) - CHAR_LENGTH(first_name) - CHAR_LENGTH(last_name) > 0 THEN SUBSTRING(clean_name, CHAR_LENGTH(first_name) + 2, CHAR_LENGTH(clean_name) - CHAR_LENGTH(first_name) - CHAR_LENGTH(last_name) -2)
    ELSE NULL
END;

# geolocation

# DROP TABLE geolocation;

CREATE TABLE geolocation(  
    name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/geolocation.csv'
INTO TABLE geolocation
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

# turing

# DROP TABLE turing;

CREATE TABLE turing(  
    name VARCHAR(255),
    year int
);

LOAD DATA INFILE '/Users/andrewliu/Desktop/class/CS170AB/170A-HW1/data/turing.csv'
INTO TABLE turing
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
