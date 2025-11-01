let button = document.querySelector('#submit')

async function passwordChanging(e){
    e.preventDefault()
    try{
        const response = await fetch("http://127.0.0.1:8000/auth/password_change", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          password: document.querySelector("#password").value,
          confirm_password: document.querySelector("#confirmPassword").value
        })
        })
        if (!response.ok) {
            const errorData = await response.json();
            alert(`Error: ${errorData.message || 'Something went wrong'}`);
            return; // stop execution
        }
        const data = await response.json();
        localStorage.removeItem('email')
        alert('password changed');
        }
    catch(err){
        console.log(err);    
    }
}

button.addEventListener('click',passwordChanging)