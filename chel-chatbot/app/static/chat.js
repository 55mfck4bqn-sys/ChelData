async function sendMsg() {
    const box = document.getElementById("chatbox");
    const msg = document.getElementById("msg").value;
    document.getElementById("msg").value = "";

    box.innerHTML += `<div class='user'>${msg}</div>`;

    const res = await fetch("/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: msg })
    });

    const data = await res.json();
    box.innerHTML += `<div class='bot'>${data.answer}</div>`;
}
