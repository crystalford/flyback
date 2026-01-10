const fetchFill = async (container) => {
  const publisherId = container.dataset.publisherId;
  const size = container.dataset.size;

  const response = await fetch("/v1/fill", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      publisher_id: publisherId,
      size
    })
  });

  if (!response.ok) {
    throw new Error("Failed to load creative");
  }

  return response.json();
};

const mountCreative = (container, config) => {
  const cruRoot = document.createElement("div");
  cruRoot.className = "flyback-cru";
  cruRoot.dataset.flybackCru = "true";
  cruRoot.dataset.config = JSON.stringify(config);
  cruRoot.style.border = "1px solid #d0d7de";
  cruRoot.style.borderRadius = "8px";
  cruRoot.style.padding = "16px";
  cruRoot.style.fontFamily = "system-ui, sans-serif";

  container.replaceChildren(cruRoot);
};

const ensureCreativeScript = () => {
  if (document.querySelector("script[data-flyback-creative]")) {
    return;
  }

  const script = document.createElement("script");
  script.src = "/creative.js";
  script.async = true;
  script.dataset.flybackCreative = "true";
  document.body.appendChild(script);
};

const init = async () => {
  const containers = document.querySelectorAll("[data-flyback-container]");

  for (const container of containers) {
    try {
      const { config } = await fetchFill(container);
      mountCreative(container, config);
    } catch (error) {
      console.error("Flyback runtime failed", error);
      container.textContent = "Flyback failed to load.";
    }
  }

  ensureCreativeScript();
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
