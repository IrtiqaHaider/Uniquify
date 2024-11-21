import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import 'bootstrap/dist/css/bootstrap.min.css';
import FileUpload from "./Components/fileupload";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <FileUpload></FileUpload>
  </StrictMode>
);
