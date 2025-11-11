const TOKEN_KEY = 'safedata_token';
const API_BASE_URL = 'http://127.0.0.1:8000';

let button = document.querySelector('.btn-custom')

async function LoginUser(e){
    e.preventDefault()
    const form = new URLSearchParams();
    form.append("username", document.querySelector("#name").value);
    form.append("password", document.querySelector("#password").value);
    try{
        const response = await fetch(`${API_BASE_URL}/auth/signin`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: form
        })
        if (!response.ok) {
            const errorData = await response.json();
            alert(`Error: ${errorData.message || 'Something went wrong'}`);
            return;
        }

        const data = await response.json();
        localStorage.setItem(TOKEN_KEY, data.access_token);
        alert('Sign in successfully!');
        window.location.href = './dashboard.html';
        }
    catch(err){
        console.log(err);    
    }
}

button.addEventListener('click',LoginUser)