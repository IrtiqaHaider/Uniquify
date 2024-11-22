import React, { useState } from "react";
import axios from "axios";

const FileUpload: React.FC = () => {
  const [fileName, setFileName] = useState<string>("");
  const [message, setMessage] = useState<string>("");
  const [processing, setProcessing] = useState<boolean>(false);
  const [filePath, setFilePath] = useState<string>("");

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setFileName(file.name);
      const formData = new FormData();
      formData.append("file", file);
      setProcessing(true);
      setMessage(""); // Reset the message during processing

      try {
        // Send the file to the backend for processing
        const response = await axios.post(
          "https://45354bbd26a5.apps-tunnel.monday.app/upload", // Use the live backend URL
          formData,
          {
            headers: {
              "Content-Type": "multipart/form-data",
            },
          }
        );

        // Extract response data
        const { message, file: filePath } = response.data;

        console.log("Backend Response - Message:", message);
        console.log("File Path:", filePath); // Verify this file path

        // Update the frontend message
        setMessage(message);
        setFilePath(filePath);

        if (!filePath) {
          setTimeout(() => {
            window.location.reload();
          }, 3000); // Adjust the time as needed
        }
      } catch (error: any) {
        console.error("Error during file upload:", error);

        // Handle errors and update the message
        const errorMessage =
          error.response?.data?.message ||
          "An error occurred while processing the file.";
        setMessage(errorMessage);
      } finally {
        setProcessing(false);
      }
    }
  };

  const handleDownload = async () => {
    console.log("file path: ", filePath);
    if (filePath) {
      const downloadUrl = `https://45354bbd26a5.apps-tunnel.monday.app${filePath}`;
      console.log("Download link: ", downloadUrl);

      // Fetch the file as a Blob
      try {
        const response = await fetch(downloadUrl);
        if (response.ok) {
          const blob = await response.blob();
          const fileName = filePath.split("/").pop() || "processed_file.csv";

          // Create a link element
          const link = document.createElement("a");
          link.href = URL.createObjectURL(blob);
          link.setAttribute("download", fileName);
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
          //setFilePath("\0");

          window.location.reload();
        } else {
          console.error("Failed to fetch the file");
        }
      } catch (error) {
        console.error("Error downloading the file:", error);
      }
    }
  };

  return (
    <div className="d-flex justify-content-center align-items-center vh-100 bg-light">
      <div className="text-center">
        <input
          type="file"
          id="file-input"
          accept=".csv, .xlsx, .xls"
          onChange={handleFileUpload}
          style={{ display: "none" }}
        />
        <label
          htmlFor="file-input"
          className="btn btn-primary btn-lg p-4 shadow-lg rounded"
        >
          {fileName ? fileName : "Click to Upload CSV/Excel File"}
        </label>
        <div className="mt-4">
          {processing ? (
            <h5>Processing...</h5>
          ) : (
            <h5
              className={
                message.includes("Error") ? "text-danger" : "text-success"
              }
            >
              {message}
            </h5>
          )}
        </div>

        {/* Conditional rendering of download button */}
        {filePath && !processing && !message.includes("Error") && (
          <div className="mt-4">
            <button className="btn btn-success btn-lg" onClick={handleDownload}>
              Download Processed File
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default FileUpload;
