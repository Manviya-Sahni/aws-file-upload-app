from flask import Flask, request, redirect, render_template, session, send_file
import bcrypt
import sqlite3
from datetime import datetime
from werkzeug.utils import secure_filename
import boto3
from botocore.client import Config
import io

app = Flask(__name__)
app.secret_key = "secret"

# ✅ S3 CONFIG
s3 = boto3.client(
    's3',
    region_name='ap-south-1',
    config=Config(signature_version='s3v4')
)

BUCKET_NAME = 'manviyasahni-file-upload-app'
DB_NAME = "database.db"

# ---------------- DB ----------------

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            password TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            filename TEXT,
            filepath TEXT,
            uploaded_at TEXT
        )
    ''')

    conn.commit()
    conn.close()

# ---------------- ROUTES ----------------

@app.route('/')
def home():
    return redirect('/login')


@app.route('/register', methods=['GET', 'POST'])
def register():
    error = None

    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password'].encode('utf-8')

        hashed = bcrypt.hashpw(password, bcrypt.gensalt()).decode('utf-8')

        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("INSERT INTO users (email, password) VALUES (?, ?)", (email, hashed))
            conn.commit()
            conn.close()
            return redirect('/login')
        except:
            error = "User already exists"

    return render_template('register.html', error=error)


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None

    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password'].encode('utf-8')

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email=?", (email,))
        user = cur.fetchone()
        conn.close()

        if user and bcrypt.checkpw(password, user['password'].encode('utf-8')):
            session['user'] = email
            session['user_id'] = user['id']
            return redirect('/dashboard')
        else:
            error = "Invalid email or password"

    return render_template('login.html', error=error)


@app.route('/dashboard')
def dashboard():
    if 'user' not in session:
        return redirect('/login')

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM files WHERE user_id=?", (session['user_id'],))
    files = cur.fetchall()
    conn.close()

    return render_template('dashboard.html', user=session['user'], files=files)


@app.route('/upload', methods=['POST'])
def upload():
    if 'user' not in session:
        return redirect('/login')

    file = request.files['file']

    if file.filename == '':
        return "No file selected"

    filename = secure_filename(file.filename).replace(" ", "_")

    # S3 key
    s3_key = f"{session['user_id']}/{filename}"

    # Upload to S3
    s3.upload_fileobj(file, BUCKET_NAME, s3_key)

    # Save metadata
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO files (user_id, filename, filepath, uploaded_at) VALUES (?, ?, ?, ?)",
        (session['user_id'], filename, s3_key, datetime.now())
    )
    conn.commit()
    conn.close()

    return redirect('/dashboard')


@app.route("/download/<int:file_id>")
def download(file_id):
    if "user" not in session:
        return redirect("/login")

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM files WHERE id=?", (file_id,))
    file = cur.fetchone()
    conn.close()

    if not file or file["user_id"] != session["user_id"]:
        return "Unauthorized"

    key = file["filepath"]

    # Fetch file from S3
    s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    file_data = s3_object["Body"].read()

    return send_file(
        io.BytesIO(file_data),
        download_name=file["filename"],
        as_attachment=True
    )

# ---------------- MAIN ----------------

if __name__ == '__main__':
    print("Starting Flask app...")
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)