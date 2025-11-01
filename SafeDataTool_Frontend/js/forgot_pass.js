let button = document.querySelector('#submit')

async function sendingOTP(e){
    e.preventDefault()
    let email = document.querySelector('#email').value
    console.log(email);
    
    try{
        const response = await fetch(`http://127.0.0.1:8000/auth/forgotpassword_email/${email}`, {
            method: "POST"
        })
        if (!response.ok) {
            const errorData = await response.json();
            alert(`Error: ${errorData.message || 'Something went wrong'}`);
            return;
        }

        const data = await response.json();
        alert(data['message']);
        localStorage.setItem('email',email)
        window.location.href = '../html/otp.html'
        }
    catch(err){
        console.log(err);    
    }
}

button.addEventListener('click',sendingOTP)