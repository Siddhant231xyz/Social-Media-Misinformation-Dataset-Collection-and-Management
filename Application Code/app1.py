from flask import Flask, render_template, request, jsonify
import psycopg2

app = Flask(__name__)

# Function to connect to the PostgreSQL database
def connect_db():
    conn = psycopg2.connect(
        dbname="Final",
        user="postgres",
        password="1111",
        host="localhost",
        port="1111"
    )
    return conn

# Homepage route
@app.route('/')
def index():
    return render_template('index1.html')

# Route to handle form submission
@app.route('/search', methods=['POST'])
def search():
    search_query = request.form['search_query']
    search_type = request.form['search_type']

    if search_type == 'user':
        return display_user_profile(search_query)
    elif search_type == 'post':
        return display_post_content(search_query)
    elif search_type == 'post_metadata':
        # Call function to display post metadata
        return display_post_metadata(search_query)
    elif search_type == 'post_source':
        # Call function to display post source
        return display_post_source(search_query)
    elif search_type == 'post_reach_engagement':
        # Call function to display post reach engagement
        return display_post_reach_engagement(search_query)
    elif search_type == 'post_analysis':
        # Call function to display post analysis
        return display_post_analysis(search_query)
    elif search_type == 'user_political_affiliation':
        # Call function to display user political affiliation
        return display_user_political_affiliation(search_query)
    elif search_type == 'user_trust_score':
        # Call function to display user trust score
        return display_user_trust_score(search_query)
    elif search_type == 'user_posts':
        # Call function to display user posts
        return display_user_posts(search_query)
    elif search_type == 'user_comments':
        # Call function to display user comments
        return display_user_comments(search_query)
    elif search_type == 'commentor_profile':
        # Call function to display commentor profile
        return display_commentor_profile(search_query)
    elif search_type == 'commentor_political_affiliation':
        # Call function to display commentor political affiliation
        return display_commentor_political_affiliation(search_query)
    elif search_type == 'commentor_trust_score':
        # Call function to display commentor trust score
        return display_commentor_trust_score(search_query)
    elif search_type == 'commentor_comments':
        # Call function to display commentor comments
        return display_commentor_comments(search_query)
    elif search_type == 'commentor_interactions':
        # Call function to display commentor interactions
        return display_commentor_interactions(search_query)
    else:
        return jsonify({'error': 'Invalid search type'})


# Route to display user profile
@app.route('/user/<user_id>')
def display_user_profile(user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM User_Profile WHERE User_ID = %s", (user_id,))
        user_profile = cursor.fetchone()

        if not user_profile:
            return jsonify({'error': 'No user found with the provided ID'})

        column_names = [description[0] for description in cursor.description]
        user_profile_dict = dict(zip(column_names, user_profile))

        return render_template('user_profile1.html', user_profile=user_profile_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()



# Route to display post content
@app.route('/post/<post_id>')
def display_post_content(post_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Post_Content WHERE Post_ID = %s", (post_id,))
        post_content = cursor.fetchone()

        if not post_content:
            return jsonify({'error': 'No post found with the provided ID'})

        column_names = [description[0] for description in cursor.description]
        post_content_dict = dict(zip(column_names, post_content))

        return render_template('post_content.html', post_content=post_content_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Route to display post metadata
@app.route('/post_metadata/<post_id>')
def display_post_metadata(post_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Post_Metadata WHERE Post_ID = %s", (post_id,))
        post_metadata = cursor.fetchone()

        if not post_metadata:
            return jsonify({'error': 'No post metadata found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        post_metadata_dict = dict(zip(column_names, post_metadata))

        return render_template('post_metadata.html', post_metadata=post_metadata_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Example route for Post_Source table
@app.route('/post_source/<source_id>')
def display_post_source(source_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Post_Source WHERE Source_ID = %s", (source_id,))
        post_source = cursor.fetchone()

        if not post_source:
            return jsonify({'error': 'No post source found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        post_source_dict = dict(zip(column_names, post_source))

        return render_template('post_source.html', post_source=post_source_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Route to display post reach engagement
@app.route('/post_reach_engagement/<post_id>')
def display_post_reach_engagement(post_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Post_Reach_Engagement WHERE Post_ID = %s", (post_id,))
        post_reach_engagement = cursor.fetchone()

        if not post_reach_engagement:
            return jsonify({'error': 'No post reach engagement found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        post_reach_engagement_dict = dict(zip(column_names, post_reach_engagement))

        return render_template('post_reach_engagement.html', post_reach_engagement=post_reach_engagement_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Route to display post analysis
@app.route('/post_analysis/<post_id>')
def display_post_analysis(post_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Post_Analysis WHERE Post_ID = %s", (post_id,))
        post_analysis = cursor.fetchone()

        if not post_analysis:
            return jsonify({'error': 'No post analysis found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        post_analysis_dict = dict(zip(column_names, post_analysis))

        return render_template('post_analysis.html', post_analysis=post_analysis_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()


# Route to display user political affiliation
@app.route('/user_political_affiliation/<user_id>')
def display_user_political_affiliation(user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM User_Political_Affiliation WHERE User_ID = %s", (user_id,))
        user_political_affiliation = cursor.fetchone()

        if not user_political_affiliation:
            return jsonify({'error': 'No user political affiliation found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        user_political_affiliation_dict = dict(zip(column_names, user_political_affiliation))

        return render_template('user_political_affiliation.html', user_political_affiliation=user_political_affiliation_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Route to display user trust score
@app.route('/user_trust_score/<user_id>')
def display_user_trust_score(user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM User_Trust_Score WHERE User_ID = %s", (user_id,))
        user_trust_score = cursor.fetchone()

        if not user_trust_score:
            return jsonify({'error': 'No user trust score found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        user_trust_score_dict = dict(zip(column_names, user_trust_score))

        return render_template('user_trust_score1.html', user_trust_score=user_trust_score_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()



# Route to display user posts
@app.route('/user_posts/<user_id>')
def display_user_posts(user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM User_Posts WHERE User_ID = %s", (user_id,))
        user_posts = cursor.fetchall()

        if not user_posts:
            return jsonify({'error': 'No user posts found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        user_posts_list = [{column_names[i]: row[i] for i in range(len(column_names))} for row in user_posts]

        return render_template('user_posts.html', user_posts=user_posts_list)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()


# Route to display user comments
@app.route('/user_comments/<user_id>')
def display_user_comments(user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM User_Comments WHERE User_ID = %s", (user_id,))
        user_comments = cursor.fetchall()

        if not user_comments:
            return jsonify({'error': 'No user comments found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        user_comments_list = [{column_names[i]: row[i] for i in range(len(column_names))} for row in user_comments]

        return render_template('user_comments.html', user_comments=user_comments_list)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Route to display commentor profile
@app.route('/commentor_profile/<commentor_user_id>')
def display_commentor_profile(commentor_user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Commentor_Profile WHERE Commentor_User_ID = %s", (commentor_user_id,))
        commentor_profile = cursor.fetchall()

        if not commentor_profile:
            return jsonify({'error': 'No commentor profile found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        commentor_profile_dict = [{column_names[i]: row[i] for i in range(len(column_names))} for row in commentor_profile]

        return render_template('commentor_profile.html', commentor_profile=commentor_profile_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()


# Route to display commentor political affiliation
@app.route('/commentor_political_affiliation/<commentor_user_id>')
def display_commentor_political_affiliation(commentor_user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Commentor_Political_Affiliation WHERE Commentor_User_ID = %s", (commentor_user_id,))
        commentor_political_affiliation = cursor.fetchone()

        if not commentor_political_affiliation:
            return jsonify({'error': 'No commentor political affiliation found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        commentor_political_affiliation_dict = dict(zip(column_names, commentor_political_affiliation))

        return render_template('commentor_political_affiliation.html', commentor_political_affiliation=commentor_political_affiliation_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()
        
# Route to display commentor trust score
@app.route('/commentor_trust_score/<commentor_user_id>')
def display_commentor_trust_score(commentor_user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Commentor_Trust_Score WHERE Commentor_User_ID = %s", (commentor_user_id,))
        commentor_trust_score = cursor.fetchone()

        if not commentor_trust_score:
            return jsonify({'error': 'No commentor trust score found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        commentor_trust_score_dict = dict(zip(column_names, commentor_trust_score))

        return render_template('commentor_trust_score.html', commentor_trust_score=commentor_trust_score_dict)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# Route to display commentor comments
@app.route('/commentor_comments/<commentor_user_id>')
def display_commentor_comments(commentor_user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Commentor_Comments WHERE Commentor_User_ID = %s", (commentor_user_id,))
        commentor_comments = cursor.fetchall()

        if not commentor_comments:
            return jsonify({'error': 'No commentor comments found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        commentor_comments_list = [{column_names[i]: row[i] for i in range(len(column_names))} for row in commentor_comments]

        return render_template('commentor_comments.html', commentor_comments=commentor_comments_list)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()


# Route to display commentor interactions
@app.route('/commentor_interactions/<commentor_user_id>')
def display_commentor_interactions(commentor_user_id):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM Commentor_Interactions WHERE Commentor_User_ID = %s", (commentor_user_id,))
        commentor_interactions = cursor.fetchall()

        if not commentor_interactions:
            return jsonify({'error': 'No commentor interactions found for the provided ID'})

        column_names = [description[0] for description in cursor.description]
        commentor_interactions_list = [{column_names[i]: row[i] for i in range(len(column_names))} for row in commentor_interactions]

        return render_template('commentor_interactions.html', commentor_interactions=commentor_interactions_list)

    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        cursor.close()
        conn.close()




if __name__ == '__main__':
    app.run(debug=True)
