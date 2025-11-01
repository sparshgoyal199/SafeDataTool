let button = document.querySelector('#submit')
let reset = document.querySelector('#resend')
let route;
if (localStorage.getItem("signup")) {
    route = 'signup'
}
else{
    route = 'forgotpassword'
}

function OTPcollect(){
    let a = document.querySelectorAll('.otpNumber')
    let value = ''
    for (const i of a) {
        value += i.value
    }
    return value
}

async function verifyingOTP(e){
    e.preventDefault()
    let value = OTPcollect()
    try{
        const response = await fetch(`http://127.0.0.1:8000/auth/${route}_verifying_otp/${value}`, {
            method: "POST"
        })
        if (!response.ok) {
            const errorData = await response.json();
            alert(`Error: ${errorData.message || 'Something went wrong'}`);
            return;
        }

        const data = await response.json();
        alert('OTP verified');
        // if(localStorage.getItem('email')){
        //     
        // }
        if(route == 'signup'){
            localStorage.removeItem('signup')
            localStorage.removeItem('email')
            //not in forgot route because this email will be used in new_pass page
        }
        else{
            localStorage.removeItem('forgotpassword')
        }
        window.location.href = '../html/new_pass.html'
        }
    catch(err){
        console.log(err);    
    }
}

async function resendOTP(e){
    e.preventDefault()
    let email = localStorage.getItem("email");
    
    try{
        const response = await fetch(`http://127.0.0.1:8000/auth/${route}_resend_otp/${email}`, {
            method: "POST"
        })
        if (!response.ok) {
            const errorData = await response.json();
            alert(`Error: ${errorData.message || 'Something went wrong'}`);
            return;
        }

        const data = await response.json();
        alert('OTP resent');
        // window.location.href = '../html/new_pass.html'
        }
    catch(err){
        console.log(err);    
    }
}

button.addEventListener('click',verifyingOTP)
resend.addEventListener('click',resendOTP)