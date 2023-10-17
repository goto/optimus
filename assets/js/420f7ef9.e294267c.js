"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[3220],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>f});var o=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,o,r=function(e,t){if(null==e)return{};var n,o,r={},i=Object.keys(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(o=0;o<i.length;o++)n=i[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=o.createContext({}),c=function(e){var t=o.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},p=function(e){var t=c(e.components);return o.createElement(l.Provider,{value:t},e.children)},d="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},m=o.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),d=c(n),m=r,f=d["".concat(l,".").concat(m)]||d[m]||u[m]||i;return n?o.createElement(f,a(a({ref:t},p),{},{components:n})):o.createElement(f,a({ref:t},p))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,a=new Array(i);a[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[d]="string"==typeof e?e:r,a[1]=s;for(var c=2;c<i;c++)a[c]=n[c];return o.createElement.apply(null,a)}return o.createElement.apply(null,n)}m.displayName="MDXCreateElement"},7563:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>a,default:()=>u,frontMatter:()=>i,metadata:()=>s,toc:()=>c});var o=n(7462),r=(n(7294),n(3905));const i={},a="Applying Job Specifications",s={unversionedId:"client-guide/applying-job-specifications",id:"client-guide/applying-job-specifications",title:"Applying Job Specifications",description:"Once you have the job specifications ready, let\u2019s try to deploy the jobs to the server by running this command:",source:"@site/docs/client-guide/applying-job-specifications.md",sourceDirName:"client-guide",slug:"/client-guide/applying-job-specifications",permalink:"/optimus/docs/client-guide/applying-job-specifications",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/applying-job-specifications.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1697515511,formattedLastUpdatedAt:"Oct 17, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Verifying the Jobs",permalink:"/optimus/docs/client-guide/verifying-jobs"},next:{title:"Uploading Job to Scheduler",permalink:"/optimus/docs/client-guide/uploading-jobs-to-scheduler"}},l={},c=[],p={toc:c},d="wrapper";function u(e){let{components:t,...n}=e;return(0,r.kt)(d,(0,o.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"applying-job-specifications"},"Applying Job Specifications"),(0,r.kt)("p",null,"Once you have the job specifications ready, let\u2019s try to deploy the jobs to the server by running this command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job replace-all --verbose\n")),(0,r.kt)("p",null,"Note: add --config flag if you are not in the same directory with your client configuration (optimus.yaml)."),(0,r.kt)("p",null,"This replace-all command works per project or namespace level and will try to compare the incoming jobs and the jobs\nin the server. You will find in the logs how many jobs are new, modified, and deleted based on the current condition."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job replace-all --verbose\n\n> Validating namespaces\nvalidation finished!\n\n> Replacing all jobs for namespaces [sample_namespace]\n> Receiving responses:\n[sample_namespace] received 1 job specs\n[sample_namespace] found 1 new, 0 modified, and 0 deleted job specs\n[sample_namespace] processing job job1\n[sample_namespace] successfully added 1 jobs\nreplace all job specifications finished!\n")),(0,r.kt)("p",null,"You might notice based on the log that Optimus tries to find which jobs are new, modified, or deleted. This is because\nOptimus will not try to process every job in every single ",(0,r.kt)("inlineCode",{parentName:"p"},"replace-all")," command for performance reasons. If you have\nneeds to refresh all of the jobs in the project from the server, regardless it has changed or not, do run the below command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job refresh --verbose\n")),(0,r.kt)("p",null,"This refresh command is not taking any specifications as a request. It will only refresh the jobs in the server."),(0,r.kt)("p",null,"Also, do notice that these ",(0,r.kt)("strong",{parentName:"p"},"replace-all")," and ",(0,r.kt)("strong",{parentName:"p"},"refresh")," commands are only for registering the job specifications in the server,\nincluding resolving the dependencies. After this, you can compile and upload the jobs to the scheduler using the\n",(0,r.kt)("inlineCode",{parentName:"p"},"scheduler upload-all")," ",(0,r.kt)("a",{parentName:"p",href:"/optimus/docs/client-guide/uploading-jobs-to-scheduler"},"command"),"."),(0,r.kt)("p",null,"Note: Currently Optimus does not provide a way to deploy only a single job through CLI. This capability is being\nsupported in the API."))}u.isMDXComponent=!0}}]);