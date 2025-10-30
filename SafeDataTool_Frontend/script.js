const form = document.getElementById("signupForm");
const password = document.getElementById("password");
const confirmPassword = document.getElementById("confirmPassword");
const errorMsg = document.getElementById("errorMsg");
const submitBtn = document.getElementById("submitBtn");

form.addEventListener("submit", (e) => {
  e.preventDefault();

  if (password.value !== confirmPassword.value) {
    errorMsg.classList.remove("hidden");
    confirmPassword.style.borderColor = "#dc2626";
  } else {
    errorMsg.classList.add("hidden");
    confirmPassword.style.borderColor = "#e5e7eb";

    submitBtn.textContent = "Signing Up...";
    submitBtn.style.background = "#10b981";

    setTimeout(() => {
      alert("Signup Successful! Welcome to NSO Safe DataPipeline 🎉");
      form.reset();
      submitBtn.textContent = "Sign Up";
      submitBtn.style.background = "linear-gradient(to right, #2563eb, #10b981)";
    }, 2000);
  }
});
