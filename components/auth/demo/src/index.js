import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import reportWebVitals from './reportWebVitals';
import {OidcProvider, OidcSecure} from "@axa-fr/react-oidc";
import {Info} from "./Info";

const configuration = {
    client_id: 'demo',
    redirect_uri: 'http://localhost:3000/auth-callback',
    silent_redirect_uri: 'http://localhost:3000/silent-auth-callback',
    scope: 'openid offline_access email',
    authority: 'http://localhost:8080',
    post_logout_redirect_uri: 'http://localhost:3000'
};

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
    <OidcProvider configuration={configuration}>
        <OidcSecure>
            <Info/>
        </OidcSecure>
    </OidcProvider>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
