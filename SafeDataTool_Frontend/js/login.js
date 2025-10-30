let button = document.querySelector('.btn-custom')

async function LoginUser(e){
    e.preventDefault()
    const form = new URLSearchParams();
    form.append("username", document.querySelector("#name").value);
    form.append("password", document.querySelector("#password").value);
    try{
        const response = await fetch("http://127.0.0.1:8000/auth/signin", {
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
        alert('Sign in successfully!');
        console.log(data);
        
        // window.location.href = '../html/new_pass.html'
        }
    catch(err){
        console.log(err);    
    }
}

button.addEventListener('click',LoginUser)