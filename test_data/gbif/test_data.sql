CREATE TABLE gbif_datasets (
                             key uuid PRIMARY KEY,
                             citation text,
                             license text,
                             uri text
);

INSERT INTO gbif_datasets VALUES ('92827b65-9987-4479-b135-7ec1bf9cf3d1','Mederos J, Pollet M, Oosterbroek P, Brosens D (2023). Tipuloidea of Martinique - 2016-2018. Version 1.10. Research Institute for Nature and Forest (INBO). Occurrence dataset https://doi.org/10.15468/s8h9pg accessed via GBIF.org on 2023-01-31.','http://creativecommons.org/publicdomain/zero/1.0/legalcode','http://www.gbif.org/dataset/92827b65-9987-4479-b135-7ec1bf9cf3d1');

CREATE TABLE occurrences (
                             gbifid bigint NOT NULL,
                             datasetkey uuid,
                             occurrenceid text,
                             kingdom text,
                             phylum text,
                             class text,
                             "order" text,
                             family text,
                             genus text,
                             species text,
                             infraspecificepithet text,
                             taxonrank text,
                             scientificname text,
                             verbatimscientificname text,
                             verbatimscientificnameauthorship text,
                             countrycode text,
                             locality text,
                             stateprovince text,
                             occurrencestatus text,
                             individualcount integer,
                             publishingorgkey text,
                             decimallatitude double precision,
                             decimallongitude double precision,
                             coordinateuncertaintyinmeters double precision,
                             coordinateprecision text,
                             elevation double precision,
                             elevationaccuracy text,
                             depth double precision,
                             depthaccuracy text,
                             eventdate timestamp without time zone,
                             day integer,
                             month integer,
                             year integer,
                             taxonkey bigint,
                             specieskey text,
                             basisofrecord text,
                             institutioncode text,
                             collectioncode text,
                             catalognumber text,
                             recordnumber text,
                             identifiedby text,
                             dateidentified timestamp without time zone,
                             license text,
                             rightsholder text,
                             recordedby text,
                             typestatus text,
                             establishmentmeans text,
                             lastinterpreted timestamp without time zone,
                             mediatype text,
                             issue text,
                             geom geometry(Point,4326)
);

INSERT INTO occurrences VALUES (4021925301, '92827b65-9987-4479-b135-7ec1bf9cf3d1', 'Mart:tipu:13801', 'Animalia', 'Arthropoda', 'Insecta', 'Diptera', 'Limoniidae', 'Rhipidia', 'Rhipidia willistoniana', NULL, 'SPECIES', 'Rhipidia willistoniana (Alexander, 1929)', 'Rhipidia (Rhipidia) willistoniana (Alexander, 1929)', NULL, 'MQ', 'Rivière Sylvestre (Le Lorrain)', 'Martinique', 'PRESENT', 156, '1cd669d0-80ea-11de-a9d0-f1765f95f18b', 14.77533, -61.06522, 30, NULL, NULL, NULL, NULL, NULL, '2018-01-27 00:00:00', 27, 1, 2018, 5066840, '5066840', 'PRESERVED_SPECIMEN', NULL, NULL, NULL, NULL, 'Jorge Mederos', NULL, 'CC0_1_0', 'dataset authors', 'Marc Pollet', NULL, NULL, '2023-01-31 08:47:46.371', NULL, NULL, '0101000020E6100000C685032159884EC02237C30DF88C2D40');
INSERT INTO occurrences VALUES (4021925302, '92827b65-9987-4479-b135-7ec1bf9cf3d1', 'Mart:tipu:13800', 'Animalia', 'Arthropoda', 'Insecta', 'Diptera', 'Limoniidae', 'Rhipidia', 'Rhipidia willistoniana', NULL, 'SPECIES', 'Rhipidia willistoniana (Alexander, 1929)', 'Rhipidia (Rhipidia) willistoniana (Alexander, 1929)', NULL, 'MQ', 'Plateau Concorde (Case-Pilote)', 'Martinique', 'PRESENT', 2, '1cd669d0-80ea-11de-a9d0-f1765f95f18b', 14.67915, -61.11447, 30, NULL, NULL, NULL, NULL, NULL, '2018-02-01 00:00:00', 1, 2, 2018, 5066840, '5066840', 'PRESERVED_SPECIMEN', NULL, NULL, NULL, NULL, 'Jorge Mederos', NULL, 'CC0_1_0', 'dataset authors', 'Marc Pollet', NULL, NULL, '2023-01-31 08:47:46.464', NULL, NULL, '0101000020E6100000C22FF5F3A68E4EC024287E8CB95B2D40');
INSERT INTO occurrences VALUES (4021925303, '92827b65-9987-4479-b135-7ec1bf9cf3d1', 'Mart:tipu:13803', 'Animalia', 'Arthropoda', 'Insecta', 'Diptera', 'Limoniidae', 'Rhipidia', 'Rhipidia willistoniana', NULL, 'SPECIES', 'Rhipidia willistoniana (Alexander, 1929)', 'Rhipidia (Rhipidia) willistoniana (Alexander, 1929)', NULL, 'MQ', 'Rivière Sylvestre (Le Lorrain)', 'Martinique', 'PRESENT', 213, '1cd669d0-80ea-11de-a9d0-f1765f95f18b', 14.77533, -61.06522, 30, NULL, NULL, NULL, NULL, NULL, '2018-01-27 00:00:00', 27, 1, 2018, 5066840, '5066840', 'PRESERVED_SPECIMEN', NULL, NULL, NULL, NULL, 'Jorge Mederos', NULL, 'CC0_1_0', 'dataset authors', 'Marc Pollet', NULL, NULL, '2023-01-31 08:47:46.507', NULL, NULL, '0101000020E6100000C685032159884EC02237C30DF88C2D40');

CREATE TABLE species
(
    taxonid integer,
    taxonrank text,
    kingdom text,
    phylum text,
    class text,
    "order" text,
    family text,
    genus text,
    canonicalname text
);

INSERT INTO species VALUES (5066840, 'species', 'Animalia', 'Arthropoda', 'Insecta', 'Diptera', 'Limoniidae', 'Rhipidia', 'Rhipidia willistoniana');
INSERT INTO species VALUES (1519912, 'genus', 'Animalia', 'Arthropoda', 'Insecta', 'Diptera', 'Limoniidae', 'Rhipidia', 'Rhipidia');
INSERT INTO species VALUES (5580, 'family', 'Animalia', 'Arthropoda', 'Insecta', 'Diptera', 'Limoniidae', NULL, 'Limoniidae');