const openQuickstartButton = document.getElementById("openQuickstart");
const copyPublisherKeyButton = document.getElementById("copyPublisherKey");
const copyAdvertiserKeyButton = document.getElementById("copyAdvertiserKey");
const publisherKeyValue = document.getElementById("publisherKeyValue");
const advertiserKeyValue = document.getElementById("advertiserKeyValue");

const QUICKSTART_TEXT = `Quickstart
1) Start the server: node server.js
2) Open ops: /ops.html?api_key=demo-publisher-key
3) Open advertiser: /advertiser.html?api_key=demo-advertiser-key
4) Trigger conversions via the creative buttons.`;

const copyText = async (text) => {
  if (!text) {
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    document.body.removeChild(textarea);
  }
};

openQuickstartButton?.addEventListener("click", () => {
  copyText(QUICKSTART_TEXT);
  openQuickstartButton.textContent = "Quickstart Copied";
  setTimeout(() => {
    openQuickstartButton.textContent = "Open Quickstart";
  }, 1500);
});

copyPublisherKeyButton?.addEventListener("click", () => {
  copyText(publisherKeyValue?.textContent || "");
});

copyAdvertiserKeyButton?.addEventListener("click", () => {
  copyText(advertiserKeyValue?.textContent || "");
});
