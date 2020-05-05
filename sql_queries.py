import config

#Fact Table

CREATE_SONGPLAYS_TABLE = """ CREATE TABLE IF NOT EXISTS  songplays
                            (songplay_id      INT IDENTITY(1, 1)     NOT NULL,
                            start_time        TIMESTAMP,
                            user_id           INT,
                            level             VARCHAR NOT NULL,
                            song_id           VARCHAR,
                            artist_id         VARCHAR,
                            session_id        INT NOT NULL,
                            location          VARCHAR,
                            user_agent        TEXT ); """

#Dimension Table
CREATE_USERS_TABLE = """ CREATE TABLE IF NOT EXISTS  users
                            (user_id     INT       NOT NULL,
                            first_name    VARCHAR,
                            last_name     VARCHAR,
                            gender        CHAR(1),
                            level         VARCHAR     NOT NULL); """

CREATE_SONGS_TABLE = """ CREATE TABLE IF NOT EXISTS  songs
                            (song_id      VARCHAR       NOT NULL ,
                            title         VARCHAR,
                            artist_id     VARCHAR,
                            year          INT,
                            duration      FLOAT); """

CREATE_ARTISTS_TABLE = """ CREATE TABLE IF NOT EXISTS  artists
                            (artist_id        VARCHAR       NOT NULL,
                            name              VARCHAR,
                            location          VARCHAR,
                            latitude          FLOAT,
                            longitude         FLOAT); """

CREATE_TIME_TABLE = """ CREATE TABLE IF NOT EXISTS  time
                            ( start_time    TIMESTAMP  NOT NULL,
                              hour          INT        NOT NULL,
                              day           INT        NOT NULL,
                              week          INT        NOT NULL,
                              month         INT        NOT NULL,
                              year          INT        NOT NULL,
                              weekday       INT        NOT NULL); """


CREATE_STAGING_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER );
"""

CREATE_STAGING_SONGS_TABLE = """
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )"""



DROP_SONGPLAYS_TABLE = """ DROP TABLE IF EXISTS songplays"""
DROP_USERS_TABLE = """ DROP TABLE IF EXISTS users"""
DROP_SONGS_TABLE = """ DROP TABLE IF EXISTS songs"""
DROP_ARTISTS_TABLE = """ DROP TABLE IF EXISTS artists"""
DROP_TIME_TABLE = """ DROP TABLE IF EXISTS time"""
DROP_STAGING_SONGS_TABLE = """ DROP TABLE IF EXISTS staging_songs"""
DROP_STAGING_EVENTS_TABLE = """ DROP TABLE IF EXISTS staging_events"""

#insert
SONGS_TABLE_INSERT = """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
"""

ARTISTS_TABLE_INSERT = """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
"""

USERS_TABLE_INSERT = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
"""

TIME_TABLE_INSERT = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT   DISTINCT(start_time)                AS start_time,
             EXTRACT(hour FROM start_time)       AS hour,
             EXTRACT(day FROM start_time)        AS day,
             EXTRACT(week FROM start_time)       AS week,
             EXTRACT(month FROM start_time)      AS month,
             EXTRACT(year FROM start_time)       AS year,
             EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
"""

SONGPLAYS_TABLE_INSERT = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time,
            e.userId        AS user_id,
            e.level         AS level,
            s.song_id       AS song_id,
            s.artist_id     AS artist_id,
            e.sessionId     AS session_id,
            e.location      AS location,
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  =  'NextSong'
"""

#Stage_tables_to_Redshift
COPY_STAGING_EVENTS = f"""
    copy staging_events from '{config.SPARKIFY_LOG_DATA}'
    credentials 'aws_iam_role={config.DWH_ROLE_ARN}'
    region 'us-west-2' format as JSON '{config.SPARKIFY_LOG_JSONPATH}'
    timeformat as 'epochmillisecs';
"""

COPY_STAGING_SONGS = f"""
    copy staging_songs from '{config.SPARKIFY_SONG_DATA}'
    credentials 'aws_iam_role={config.DWH_ROLE_ARN}'
    region 'us-west-2' format as JSON 'auto';
"""



# queries LIST
DROP_QUERIES = [DROP_SONGPLAYS_TABLE,
                DROP_ARTISTS_TABLE,
                DROP_USERS_TABLE,
                DROP_SONGS_TABLE,
                DROP_TIME_TABLE,
                DROP_STAGING_EVENTS_TABLE,
                DROP_STAGING_SONGS_TABLE]

CREATE_QUERIES = [CREATE_SONGPLAYS_TABLE,
                  CREATE_ARTISTS_TABLE,
                  CREATE_SONGS_TABLE,
                  CREATE_USERS_TABLE,
                  CREATE_TIME_TABLE,
                  CREATE_STAGING_EVENTS_TABLE,
                  CREATE_STAGING_SONGS_TABLE]

INSERT_TABLE_QUERUES = [SONGPLAYS_TABLE_INSERT,
                        USERS_TABLE_INSERT,
                        SONGS_TABLE_INSERT,
                        ARTISTS_TABLE_INSERT,
                        TIME_TABLE_INSERT]

COPY_TABLE_QUERIES = [COPY_STAGING_EVENTS, COPY_STAGING_SONGS]
