let button = document.querySelector('.btn-custom')

async function CreatingUser(e){
    e.preventDefault()
    try{
        const response = await fetch("http://127.0.0.1:8000/auth/signup", {
        method: "POST",
        /*this method will coordinate with the fastapi endpoint method in the case of same endpoints */
        /*to tell the server what type of operation it wants to do */
        headers: { "Content-Type": "application/json" },
        /*sending data in the json data*/
        /*so that fastapi knows how to read the data*/
        /*fastapi use json.loads() automatically when it sees application/json */
        body: JSON.stringify({
          username: document.querySelector("#name").value,
          email: document.querySelector("#email").value,
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
        localStorage.setItem("email",document.querySelector("#email").value)
        localStorage.setItem("signup",true)
        alert(data['message']);
        window.location.href = '../html/otp.html'
        }
    catch(err){
        console.log(err);    
    }
}

button.addEventListener('click',CreatingUser)

