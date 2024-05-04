-- Create a master table
CREATE TABLE master_data (
    Comment_Count INTEGER, Political_Affiliation VARCHAR, Comment_ID VARCHAR, Age INTEGER, Keywords VARCHAR,
    Interaction_ID VARCHAR, Source_Credibility_Score INTEGER, Like_Count INTEGER, Gender VARCHAR, Language VARCHAR,
 	Education_Level VARCHAR, Trust_Score INTEGER, URL VARCHAR, Location VARCHAR, Name VARCHAR, Reply_Count INTEGER,
	Interaction_Target_ID VARCHAR, Likes_Count INTEGER, User_ID VARCHAR, Category VARCHAR, Source_ID VARCHAR,
    Interaction_Timestamp TIMESTAMP, Source_Type VARCHAR, Share_Count INTEGER, Reach INTEGER,
    Content VARCHAR, Timestamp TIMESTAMP, Sentiment VARCHAR, Interaction_Content VARCHAR, Title VARCHAR, Post_ID VARCHAR,
 	Commentor_User_ID VARCHAR, Job_Title VARCHAR, Engagement INTEGER, Interaction_Type VARCHAR, Tags VARCHAR
);

-- Import CSV data into the master table
COPY master_data from 'C:\Users\admin\Desktop\misinformation_data.csv' WITH CSV HEADER;

-- Create BCNF Table
-- Post_Content Table
CREATE TABLE Post_Content (
    Post_ID VARCHAR PRIMARY KEY,
    Title VARCHAR,
    Content VARCHAR,
    URL VARCHAR
);

-- Inserting data into Post_Content
INSERT INTO Post_Content (Post_ID, Title, Content, URL)
SELECT DISTINCT Post_ID, Title, Content, URL
FROM master_data;


--CREATE Remaining BCNF Tables
-- Post_Content Table
CREATE TABLE Post_Content (
    Post_ID VARCHAR PRIMARY KEY,
    Title VARCHAR,
    Content VARCHAR,
    URL VARCHAR
);

-- Post_Metadata Table with DELETE CASCADE on Post_ID
CREATE TABLE Post_Metadata (
    Post_ID VARCHAR,
    Source_ID VARCHAR,
    Timestamp TIMESTAMP,
    Source_Type VARCHAR,
    Source_Credibility_Score INTEGER,
    Reach INTEGER,
    Engagement INTEGER,
    Keywords VARCHAR,
    Sentiment VARCHAR,
    Language VARCHAR,
    PRIMARY KEY (Post_ID, Source_ID),
    FOREIGN KEY (Post_ID) REFERENCES Post_Content(Post_ID) ON DELETE CASCADE,
    FOREIGN KEY (Source_ID) REFERENCES Post_Source(Source_ID)
);

-- Post_Source Table
CREATE TABLE Post_Source (
    Source_ID VARCHAR PRIMARY KEY,
    Name VARCHAR,
    URL VARCHAR
);

-- Post_Reach_Engagement Table with DELETE CASCADE on Post_ID
CREATE TABLE Post_Reach_Engagement (
    Post_ID VARCHAR PRIMARY KEY,
    Reach INTEGER,
    Engagement INTEGER,
    Share_Count INTEGER,
    FOREIGN KEY (Post_ID) REFERENCES Post_Content(Post_ID) ON DELETE CASCADE
);

-- Post_Analysis Table with DELETE CASCADE on Post_ID
CREATE TABLE Post_Analysis (
    Post_ID VARCHAR PRIMARY KEY,
    Keywords VARCHAR,
    Sentiment VARCHAR,
    Language VARCHAR,
    Tags VARCHAR,
    Category VARCHAR,
    FOREIGN KEY (Post_ID) REFERENCES Post_Content(Post_ID) ON DELETE CASCADE
);

-- User_Profile Table
CREATE TABLE User_Profile (
    User_ID VARCHAR PRIMARY KEY,
    Age INTEGER,
    Gender VARCHAR,
    Location VARCHAR,
    Job_Title VARCHAR,
    Education_Level VARCHAR
);

-- User_Political_Affiliation Table with NO ACTION on delete
CREATE TABLE User_Political_Affiliation (
    User_ID VARCHAR PRIMARY KEY,
    Political_Affiliation VARCHAR,
    FOREIGN KEY (User_ID) REFERENCES User_Profile(User_ID)
);

-- User_Trust_Score Table with DELETE CASCADE on User_ID
CREATE TABLE User_Trust_Score (
    User_ID VARCHAR PRIMARY KEY,
    Trust_Score INTEGER,
    FOREIGN KEY (User_ID) REFERENCES User_Profile(User_ID) ON DELETE CASCADE
);

-- User_Posts Table with DELETE CASCADE on User_ID
CREATE TABLE User_Posts (
    User_ID VARCHAR,
    Post_ID VARCHAR,
    Share_Count INTEGER,
    Comment_Count INTEGER,
    Like_Count INTEGER,
    PRIMARY KEY (User_ID, Post_ID),
    FOREIGN KEY (User_ID) REFERENCES User_Profile(User_ID) ON DELETE CASCADE,
    FOREIGN KEY (Post_ID) REFERENCES Post_Content(Post_ID)
);

-- User_Comments Table
CREATE TABLE User_Comments (
    User_ID VARCHAR,
    Comment_ID VARCHAR,
    Likes_Count INTEGER,
    Reply_Count INTEGER,
    Share_Count INTEGER,
    PRIMARY KEY (User_ID, Comment_ID),
    FOREIGN KEY (User_ID) REFERENCES User_Profile(User_ID)
);

-- Commentor_Profile Table
CREATE TABLE Commentor_Profile (
    Commentor_User_ID VARCHAR PRIMARY KEY,
    Age INTEGER,
    Gender VARCHAR,
    Location VARCHAR,
    Education_Level VARCHAR
);

-- Commentor_Political_Affiliation Table with SET NULL on delete
CREATE TABLE Commentor_Political_Affiliation (
    Commentor_User_ID VARCHAR PRIMARY KEY,
    Political_Affiliation VARCHAR,
    FOREIGN KEY (Commentor_User_ID) REFERENCES Commentor_Profile(Commentor_User_ID) ON DELETE SET NULL
);

-- Commentor_Trust_Score Table with DELETE CASCADE on Commentor_User_ID
CREATE TABLE Commentor_Trust_Score (
    Commentor_User_ID VARCHAR PRIMARY KEY,
    Trust_Score INTEGER,
    FOREIGN KEY (Commentor_User_ID) REFERENCES Commentor_Profile(Commentor_User_ID) ON DELETE CASCADE
);

-- Commentor_Comments Table
CREATE TABLE Commentor_Comments (
    Commentor_User_ID VARCHAR,
    Comment_ID VARCHAR,
    Likes_Count INTEGER,
    Reply_Count INTEGER,
    Share_Count INTEGER,
    PRIMARY KEY (Commentor_User_ID, Comment_ID),
    FOREIGN KEY (Commentor_User_ID) REFERENCES Commentor_Profile(Commentor_User_ID)
);

-- Commentor_Interactions Table
CREATE TABLE Commentor_Interactions (
    Interaction_ID VARCHAR,
    Commentor_User_ID VARCHAR,
    Interaction_Type VARCHAR,
    Interaction_Target_ID VARCHAR,
    Interaction_Timestamp TIMESTAMP,
    Interaction_Content VARCHAR,
    PRIMARY KEY (Interaction_ID, Commentor_User_ID),
    FOREIGN KEY (Commentor_User_ID) REFERENCES Commentor_Profile(Commentor_User_ID)
);


--INSERT Remaining BCNF Tables
-- Inserting data into Post_Content
INSERT INTO Post_Content (Post_ID, Title, Content, URL)
SELECT DISTINCT Post_ID, Title, Content, URL
FROM master_data;

-- Inserting data into Post_Metadata
INSERT INTO Post_Metadata (Post_ID, Source_ID, Timestamp, Source_Type, Source_Credibility_Score, Reach, Engagement, Keywords, Sentiment, Language)
SELECT DISTINCT Post_ID, Source_ID, Timestamp, Source_Type, Source_Credibility_Score, Reach, Engagement, Keywords, Sentiment, Language
FROM master_data;

-- Inserting data into Post_Source
INSERT INTO Post_Source (Source_ID, Name, URL)
SELECT DISTINCT Source_ID, Name, URL
FROM master_data;

-- Inserting data into Post_Reach_Engagement
INSERT INTO Post_Reach_Engagement (Post_ID, Reach, Engagement, Share_Count)
SELECT DISTINCT Post_ID, Reach, Engagement, Share_Count
FROM master_data;

-- Inserting data into Post_Analysis
INSERT INTO Post_Analysis (Post_ID, Keywords, Sentiment, Language, Tags, Category)
SELECT DISTINCT Post_ID, Keywords, Sentiment, Language, Tags, Category
FROM master_data;

-- Inserting data into User_Profile
INSERT INTO User_Profile (User_ID, Age, Gender, Location, Job_Title, Education_Level)
SELECT DISTINCT User_ID, Age, Gender, Location, Job_Title, Education_Level
FROM master_data;

-- Inserting data into User_Political_Affiliation
INSERT INTO User_Political_Affiliation (User_ID, Political_Affiliation)
SELECT DISTINCT User_ID, Political_Affiliation
FROM master_data;

-- Inserting data into User_Trust_Score
INSERT INTO User_Trust_Score (User_ID, Trust_Score)
SELECT DISTINCT User_ID, Trust_Score
FROM master_data;

-- Inserting data into User_Posts
INSERT INTO User_Posts (User_ID, Post_ID, Share_Count, Comment_Count, Like_Count)
SELECT DISTINCT User_ID, Post_ID, Share_Count, Comment_Count, Like_Count
FROM master_data;

-- Inserting data into User_Comments
INSERT INTO User_Comments (User_ID, Comment_ID, Likes_Count, Reply_Count, Share_Count)
SELECT DISTINCT User_ID, Comment_ID, Likes_Count, Reply_Count, Share_Count
FROM master_data;

-- Inserting data into Commentor_Profile
INSERT INTO Commentor_Profile (Commentor_User_ID, Age, Gender, Location, Education_Level)
SELECT DISTINCT Commentor_User_ID, Age, Gender, Location, Education_Level
FROM master_data;

-- Inserting data into Commentor_Political_Affiliation
INSERT INTO Commentor_Political_Affiliation (Commentor_User_ID, Political_Affiliation)
SELECT DISTINCT Commentor_User_ID, Political_Affiliation
FROM master_data;

-- Inserting data into Commentor_Trust_Score
INSERT INTO Commentor_Trust_Score (Commentor_User_ID, Trust_Score)
SELECT DISTINCT Commentor_User_ID, Trust_Score
FROM master_data;

-- Inserting data into Commentor_Comments
INSERT INTO Commentor_Comments (Commentor_User_ID, Comment_ID, Likes_Count, Reply_Count, Share_Count)
SELECT DISTINCT Commentor_User_ID, Comment_ID, Likes_Count, Reply_Count, Share_Count
FROM master_data;

-- Inserting data into Commentor_Interactions
INSERT INTO Commentor_Interactions (Interaction_ID, Commentor_User_ID, Interaction_Type, Interaction_Target_ID, Interaction_Timestamp, Interaction_Content)
SELECT DISTINCT Interaction_ID, Commentor_User_ID, Interaction_Type, Interaction_Target_ID, Interaction_Timestamp, Interaction_Content
FROM master_data;



--SQL Queries(Min 10 queries)
-- Deleting invalid user comments based on age
DELETE FROM User_Comments
USING User_Profile
WHERE User_Comments.User_ID = User_Profile.User_ID AND User_Profile.Age < 0;

-- Updating Trust_Score for users located in California
UPDATE User_Trust_Score
SET Trust_Score = Trust_Score + 1
WHERE User_ID IN (SELECT User_ID FROM User_Profile WHERE Location = 'California');

-- Query to select top 10 users along with their job titles, post IDs, and titles based on Share_Count
SELECT u.User_ID, u.Job_Title, p.Post_ID, p.Title
FROM User_Profile u
JOIN User_Posts up ON u.User_ID = up.User_ID
JOIN Post_Content p ON up.Post_ID = p.Post_ID
ORDER BY up.Share_Count DESC
LIMIT 10;

-- Query to find average credibility score by Source_Type where average score is greater than 3
SELECT Source_Type, AVG(Source_Credibility_Score) AS Avg_Score
FROM Post_Metadata
GROUP BY Source_Type
HAVING AVG(Source_Credibility_Score) > 3;

-- Query to select post titles and source types from Post_Content where Source_Credibility_Score is greater than 7
SELECT pc.Title, pm.Source_Type
FROM Post_Content pc
JOIN Post_Metadata pm ON pc.Post_ID = pm.Post_ID
WHERE pm.Source_Credibility_Score > 7;

-- Query to select post IDs and titles from Post_Content where Source_Credibility_Score is greater than 5
SELECT Post_ID, Title
FROM Post_Content
WHERE Post_ID IN (SELECT Post_ID FROM Post_Metadata WHERE Source_Credibility_Score > 5);

-- Query to count total users by location
SELECT Location, COUNT(User_ID) AS Total_Users
FROM User_Profile
GROUP BY Location;

-- Query to select user IDs, ages, and job titles from User_Profile table, ordered by age descending
SELECT User_ID, Age, Job_Title
FROM User_Profile
ORDER BY Age DESC;

-- Query to select all columns from User_Profile where age is greater than 79
SELECT * FROM User_Profile
WHERE Age > 79;

-- Updating Source_Type for a specific post
UPDATE post_metadata
SET source_type = 'After Update'
WHERE post_id = 'f29ddddf-61d1-481c-8c06-96cd92329929';

-- Inserting a new post into Post_Content table
INSERT INTO Post_Content (Post_ID, Title, Content, URL)
VALUES 
('e9031011-fd6d-4b33-9569-7a9b80b0a317', 
 'Happy sit pretty financial society.', 
 'Source picture lose rest owner within. Drive paper large notice.', 
 'http://camacho.com/');

-- Deleting a post from User_Posts and Post_Content tables
DELETE FROM User_Posts
WHERE Post_ID = 'e9031011-fd6d-4b33-9569-7a9b80b0a317';

DELETE FROM Post_Content
WHERE Post_ID = 'e9031011-fd6d-4b33-9569-7a9b80b0a317';



--Query execution analysis
--Before Indexing
EXPLAIN ANALYZE
SELECT Post_ID, Title
FROM Post_Content
WHERE Post_ID IN (SELECT Post_ID FROM Post_Metadata WHERE Source_Credibility_Score > 5);

EXPLAIN ANALYZE
SELECT Source_Type, AVG(Source_Credibility_Score) AS Avg_Score
FROM Post_Metadata
GROUP BY Source_Type
HAVING AVG(Source_Credibility_Score) > 3;

EXPLAIN ANALYZE
SELECT u.User_ID, u.Job_Title, p.Post_ID, p.Title
FROM User_Profile u
JOIN User_Posts up ON u.User_ID = up.User_ID
JOIN Post_Content p ON up.Post_ID = p.Post_ID
ORDER BY up.Share_Count DESC
LIMIT 10;

--Indexing

-- Creating indexes on Post_Metadata and Post_Content tables
CREATE INDEX IF NOT EXISTS idx_post_metadata_on_post_id_and_credibility ON Post_Metadata (Post_ID, Source_Credibility_Score);
CREATE INDEX IF NOT EXISTS idx_post_content_on_post_id ON Post_Content (Post_ID);

-- Creating additional indexes for optimization
CREATE INDEX idx_post_metadata_post_id_credibility ON Post_Metadata(Post_ID, Source_Credibility_Score);
CREATE INDEX idx_post_content_post_id ON Post_Content(Post_ID);
CREATE INDEX idx_source_type_avg_credibility ON Post_Metadata(Source_Type, Source_Credibility_Score);
CREATE INDEX idx_post_metadata_post_id ON Post_Metadata(Post_ID);
CREATE INDEX idx_post_metadata_source_credibility ON Post_Metadata(Source_Credibility_Score);
CREATE INDEX idx_user_posts_user_id ON User_Posts(User_ID);
CREATE INDEX idx_user_posts_post_id ON User_Posts(Post_ID);
CREATE INDEX idx_user_posts_share_count ON User_Posts(Share_Count DESC);

--After Indexing
EXPLAIN ANALYZE
SELECT Post_ID, Title
FROM Post_Content
WHERE Post_ID IN (SELECT Post_ID FROM Post_Metadata WHERE Source_Credibility_Score > 5);

EXPLAIN ANALYZE
SELECT Source_Type, AVG(Source_Credibility_Score) AS Avg_Score
FROM Post_Metadata
GROUP BY Source_Type
HAVING AVG(Source_Credibility_Score) > 3;

EXPLAIN ANALYZE
SELECT u.User_ID, u.Job_Title, p.Post_ID, p.Title
FROM User_Profile u
JOIN User_Posts up ON u.User_ID = up.User_ID
JOIN Post_Content p ON up.Post_ID = p.Post_ID
ORDER BY up.Share_Count DESC
LIMIT 10;

