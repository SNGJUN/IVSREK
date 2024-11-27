const STREAM_URL = "https://648915228a08.ap-northeast-2.playback.live-video.net/api/video/v1/ap-northeast-2.220563295011.channel.LofvRL0Ecbzt.m3u8";

// Set to store already displayed titles to prevent duplicates
const displayedTitles = new Set();

// Initialize player
registerIVSTech(videojs);
const videoJSPlayer = videojs("amazon-ivs-videojs", {
    techOrder: ["AmazonIVS"],
    controlBar: {
        playToggle: true,
        pictureInPictureToggle: false
    }
});

videoJSPlayer.ready(() => {
    const ivsPlayer = videoJSPlayer.getIVSPlayer();
    videoJSPlayer.src(STREAM_URL);

    // Log and display timed metadata
    const PlayerEventType = videoJSPlayer.getIVSEvents().PlayerEventType;
    ivsPlayer.addEventListener(PlayerEventType.TEXT_METADATA_CUE, (cue) => {
        const metadataText = cue.text;
        const position = ivsPlayer.getPosition().toFixed(2);
        console.log(
            `Player Event - TEXT_METADATA_CUE: "${metadataText}". \nObserved ${position}s after playback started.`
        );

        try {
            // Parse metadata JSON
            const metadata = JSON.parse(metadataText);

            // Section 1: Display metadata text
            document.getElementById("metadata").textContent = `Metadata at ${position}s:\n${JSON.stringify(metadata, null, 2)}`;

            // Section 1: Set primary image
            const imageElement1 = document.getElementById("metadata-image");
            if (metadata.image && imageElement1) {
                imageElement1.src = metadata.image;
                imageElement1.style.display = "block";
            } else if (imageElement1) {
                imageElement1.style.display = "none";
            }

            // Section 2: Append new title : url pair if not already displayed
            if (metadata.title && metadata.url) {
                const outputContainer = document.getElementById("metadata-output");

                if (!displayedTitles.has(metadata.title)) {
                    const newLine = document.createElement("div");

                    // Add clickable link
                    const linkElement = document.createElement("a");
                    linkElement.href = metadata.url;
                    linkElement.target = "_blank";
                    linkElement.textContent = `${metadata.title} : ${metadata.url}`;
                    linkElement.style.color = "#007bff";
                    linkElement.style.textDecoration = "underline";

                    newLine.appendChild(linkElement);
                    newLine.style.marginBottom = "5px";
                    outputContainer.appendChild(newLine);

                    displayedTitles.add(metadata.title);
                }
            }
        } catch (error) {
            console.error("Error parsing metadata:", error);
        }
    });
});
