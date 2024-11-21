import React, { useState } from "react";
import axios from "axios";

const FileUpload: React.FC = () => {
  const [fileName, setFileName] = useState<string>("");
  const [message, setMessage] = useState<string>("");
  const [processing, setProcessing] = useState<boolean>(false);

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
          "https://uniquify-backend.onrender.com/upload", // Use the live backend URL
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

        if (filePath) {
          //   // Assuming the server is running on http://localhost:5000
          //   const downloadUrl = `http://localhost:5000${filePath}`;

          //   // Use the Blob method to handle file download for the client-side
          //   const link = document.createElement("a");
          //   link.href = downloadUrl;
          //   link.setAttribute(
          //     "download",
          //     filePath.split("/").pop() || "processed_file.xlsx"
          //   );
          //   document.body.appendChild(link);
          //   link.click();
          //   link.remove();

          const downloadUrl = `https://uniquify-backend.onrender.com${filePath}`;

          // Use the Blob method to handle file download for the client-side
          const link = document.createElement("a");
          link.href = downloadUrl;
          link.setAttribute(
            "download",
            filePath.split("/").pop() || "processed_file.xlsx"
          );
          document.body.appendChild(link);
          link.click();
          link.remove();
        }

        setTimeout(() => {
          window.location.reload(); // Refresh the page
        }, 3000);
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
      </div>
    </div>
  );
};

export default FileUpload;
