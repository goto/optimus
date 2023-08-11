"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9987],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>g});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function s(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?s(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):s(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function o(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},s=Object.keys(e);for(n=0;n<s.length;n++)r=s[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(e);for(n=0;n<s.length;n++)r=s[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),c=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},u=function(e){var t=c(e.components);return n.createElement(l.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,s=e.originalType,l=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),p=c(r),m=a,g=p["".concat(l,".").concat(m)]||p[m]||d[m]||s;return r?n.createElement(g,i(i({ref:t},u),{},{components:r})):n.createElement(g,i({ref:t},u))}));function g(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var s=r.length,i=new Array(s);i[0]=m;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o[p]="string"==typeof e?e:a,i[1]=o;for(var c=2;c<s;c++)i[c]=r[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},1954:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>d,frontMatter:()=>s,metadata:()=>o,toc:()=>c});var n=r(7462),a=(r(7294),r(3905));const s={},i="Managing Secrets",o={unversionedId:"client-guide/managing-secrets",id:"client-guide/managing-secrets",title:"Managing Secrets",description:"During job execution, specific credentials are needed to access required resources, for example, BigQuery credential",source:"@site/docs/client-guide/managing-secrets.md",sourceDirName:"client-guide",slug:"/client-guide/managing-secrets",permalink:"/optimus/docs/client-guide/managing-secrets",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/managing-secrets.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1691727102,formattedLastUpdatedAt:"Aug 11, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Managing Project & Namespace",permalink:"/optimus/docs/client-guide/managing-project-namespace"},next:{title:"Installing Plugin in Client",permalink:"/optimus/docs/client-guide/installing-plugin"}},l={},c=[{value:"Registering secret",id:"registering-secret",level:2},{value:"Updating a secret",id:"updating-a-secret",level:2},{value:"Listing secrets",id:"listing-secrets",level:2}],u={toc:c},p="wrapper";function d(e){let{components:t,...r}=e;return(0,a.kt)(p,(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"managing-secrets"},"Managing Secrets"),(0,a.kt)("p",null,"During job execution, specific credentials are needed to access required resources, for example, BigQuery credential\nfor BQ to BQ tasks. Users are able to register secrets on their own, manage them, and use them in tasks and hooks.\nPlease go through ",(0,a.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/secret"},"concepts")," to know more about secrets."),(0,a.kt)("p",null,"Before we begin, let\u2019s take a look at several mandatory secrets that is used for specific use cases in Optimus."),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Secret Name"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"STORAGE"),(0,a.kt)("td",{parentName:"tr",align:null},"To store compiled jobs if needed.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"SCHEDULER_AUTH"),(0,a.kt)("td",{parentName:"tr",align:null},"Scheduler credentials. For now, since Optimus only supports Airflow, this will be Airflow ","[username:password]")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"BQ_SERVICE_ACCOUNT"),(0,a.kt)("td",{parentName:"tr",align:null},"Used for any operations involving BigQuery, such as job validation, deployment, run for jobs with BQ to BQ transformation task, as well as for managing BigQuery resources through Optimus.")))),(0,a.kt)("h2",{id:"registering-secret"},"Registering secret"),(0,a.kt)("p",null,"Register a secret by running the following command:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret set someSecret someSecretValue\n")),(0,a.kt)("p",null,"By default, Optimus will encode the secret value. However, to register a secret that has been encoded, run the following\ncommand instead:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret set someSecret encodedSecretValue --base64\n")),(0,a.kt)("p",null,"There is also a flexibility to register using an existing secret file, instead of providing the secret value in the command."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret set someSecret --file=/path/to/secret\n")),(0,a.kt)("p",null,"Secret can also be set to a specific namespace which can only be used by the jobs/resources in the namespace.\nTo register, run the following command:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret set someSecret someSecretValue --namespace someNamespace\n")),(0,a.kt)("p",null,"Please note that registering a secret that already exists will result in an error. Modifying an existing secret\ncan be done using the Update command."),(0,a.kt)("h2",{id:"updating-a-secret"},"Updating a secret"),(0,a.kt)("p",null,"The update-only flag is generally used when you explicitly only want to update a secret that already exists and doesn't want to create it by mistake."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret set someSecret someSecretValue --update-only\n")),(0,a.kt)("p",null,"It will return an error if the secret to update does not exist already."),(0,a.kt)("h2",{id:"listing-secrets"},"Listing secrets"),(0,a.kt)("p",null,"The list command can be used to show the user-defined secrets which are registered with Optimus. It will list the namespace associated with a secret."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret list\nSecrets for project: optimus-local\nNAME    |                    DIGEST                    | NAMESPACE |         DATE\n-------------+----------------------------------------------+-----------+----------------------\nsecret1   | SIBzsgUuHnExBY4qSzqcrlrb+3zCAHGu/4Fv1O8eMI8= |     *     | 2022-04-12T04:30:45Z\n")),(0,a.kt)("p",null,"It shows a digest for the encrypted secret, so as not to send the cleartext password on the network."))}d.isMDXComponent=!0}}]);