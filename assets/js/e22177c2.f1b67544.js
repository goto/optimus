"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[3807],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>g});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=a.createContext({}),p=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},u=function(e){var t=p(e.components);return a.createElement(s.Provider,{value:t},e.children)},m="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,r=e.originalType,s=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),m=p(n),d=i,g=m["".concat(s,".").concat(d)]||m[d]||c[d]||r;return n?a.createElement(g,l(l({ref:t},u),{},{components:n})):a.createElement(g,l({ref:t},u))}));function g(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=n.length,l=new Array(r);l[0]=d;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[m]="string"==typeof e?e:i,l[1]=o;for(var p=2;p<r;p++)l[p]=n[p];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},673:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>c,frontMatter:()=>r,metadata:()=>o,toc:()=>p});var a=n(7462),i=(n(7294),n(3905));const r={},l=void 0,o={unversionedId:"rfcs/simplify_plugin_maintenance",id:"rfcs/simplify_plugin_maintenance",title:"simplify_plugin_maintenance",description:"- Feature Name: Simplify Plugins",source:"@site/docs/rfcs/20220507_simplify_plugin_maintenance.md",sourceDirName:"rfcs",slug:"/rfcs/simplify_plugin_maintenance",permalink:"/optimus/docs/rfcs/simplify_plugin_maintenance",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/rfcs/20220507_simplify_plugin_maintenance.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1690956709,formattedLastUpdatedAt:"Aug 2, 2023",sidebarPosition:20220507,frontMatter:{}},s={},p=[{value:"Background :",id:"background-",level:2},{value:"Changes that trigger a new release in Optimus setup:",id:"changes-that-trigger-a-new-release-in-optimus-setup",level:3},{value:"Release dependencies as per current design",id:"release-dependencies-as-per-current-design",level:3},{value:"1. <u>Avoid Wrapping Executor Images</u>  :",id:"1-avoid-wrapping-executor-images--",level:3},{value:"2. <u>Simplify Plugin Installation</u> :",id:"2-simplify-plugin-installation-",level:3},{value:"Approach :",id:"approach-",level:2},{value:"1. <u>Avoid Wrapping Executor Images </u> :",id:"1-avoid-wrapping-executor-images---1",level:3},{value:"2. <u>Simplify Plugin Installations</u> :",id:"2-simplify-plugin-installations-",level:3},{value:"A) Plugin Manager:",id:"a-plugin-manager",level:4},{value:"B) Yaml Plugin Interface: (for client side simplification)",id:"b-yaml-plugin-interface-for-client-side-simplification",level:4},{value:"Result:",id:"result",level:2}],u={toc:p},m="wrapper";function c(e){let{components:t,...n}=e;return(0,i.kt)(m,(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Feature Name: Simplify Plugins"),(0,i.kt)("li",{parentName:"ul"},"Status: Draft"),(0,i.kt)("li",{parentName:"ul"},"Start Date: 2022-05-07"),(0,i.kt)("li",{parentName:"ul"},"Author: Saikumar")),(0,i.kt)("h1",{id:"summary"},"Summary"),(0,i.kt)("p",null,"The scope of this rfc is to simplify the release and deployment operations w.r.t the optimus plugin ecosystem."),(0,i.kt)("p",null,"The proposal here is to :"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"Avoid Wrapping Executor Images")," :",(0,i.kt)("br",{parentName:"li"}),"Decouple the executor_boot_process and the executor as separate containers where the airflow worker launches a pod with init-container (for boot process) adjacent to executor container."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"Simplfy Plugin Installation")," :",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"Server end")," : Install plugins declaratively at runtime instead of manually baking them into the optimus server image (in kubernetes setup)."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"Client end")," : Plugin interface for client-end is limited to support  Version, Survey Questions and Answers etc. that can be extracted out from the current plugin interface and maintained as yaml file which simplifies platform dependent plugin distribution for cli.")))),(0,i.kt)("h1",{id:"technical-design"},"Technical Design"),(0,i.kt)("h2",{id:"background-"},"Background :"),(0,i.kt)("h3",{id:"changes-that-trigger-a-new-release-in-optimus-setup"},"Changes that trigger a new release in Optimus setup:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Executor Image changes"),(0,i.kt)("li",{parentName:"ul"},"Executor Image Wrapper changes"),(0,i.kt)("li",{parentName:"ul"},"Plugin binary changes"),(0,i.kt)("li",{parentName:"ul"},"Optimus binary changes")),(0,i.kt)("h3",{id:"release-dependencies-as-per-current-design"},"Release dependencies as per current design"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"Executor Image release")," -> ",(0,i.kt)("inlineCode",{parentName:"li"},"Executor Wrapper Image release")," -> ",(0,i.kt)("inlineCode",{parentName:"li"},"Plugin binary release")," -> ",(0,i.kt)("inlineCode",{parentName:"li"},"Server release")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"Plugin binary release")," -> ",(0,i.kt)("inlineCode",{parentName:"li"},"Server release"))),(0,i.kt)("h3",{id:"1-avoid-wrapping-executor-images--"},"1. ",(0,i.kt)("u",null,"Avoid Wrapping Executor Images"),"  :"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"The ",(0,i.kt)("inlineCode",{parentName:"li"},"executor_boot_process")," and ",(0,i.kt)("inlineCode",{parentName:"li"},"executor")," are coupled:")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"-- Plugin repo structure\n/task/\n/task/Dockerfile           -- task_image\n/task/executor/Dockerfile  -- executor_image\n")),(0,i.kt)("p",null,"Executor Wrapper Image (Task Image) :"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"It's a wrapper around the executor_image to facilitate boot mechanism for executor."),(0,i.kt)("li",{parentName:"ul"},"The optimus binary is downloaded during buildtime of this image."),(0,i.kt)("li",{parentName:"ul"},"During runtime, it does as follow :",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Fetch assets, secrets, env from optimus server."),(0,i.kt)("li",{parentName:"ul"},"Load the env and launches the executor process.")))),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"task_image \n    | executor_image\n    | optimus-bin\n    | entrypoint.sh (load assets, env and launch executor)\n")),(0,i.kt)("p",null,"The ",(0,i.kt)("inlineCode",{parentName:"p"},"optimus-bin")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"entrypoint.sh")," are baked into the ",(0,i.kt)("inlineCode",{parentName:"p"},"task_image")," and is being maintained by task/plugin developers."),(0,i.kt)("h3",{id:"2-simplify-plugin-installation-"},"2. ",(0,i.kt)("u",null,"Simplify Plugin Installation")," :"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Plugin binaries are manually installed (baked into optimus image in kubernetes setup). "),(0,i.kt)("li",{parentName:"ul"},"Any change in plugin code demands re-creation of optimus image with new plugin binary, inturn demanding redeployment of optimus server. (in kubernetes setup)"),(0,i.kt)("li",{parentName:"ul"},"At client side, plugin binaries require support for different platforms.")),(0,i.kt)("h2",{id:"approach-"},"Approach :"),(0,i.kt)("h3",{id:"1-avoid-wrapping-executor-images---1"},"1. ",(0,i.kt)("u",null,"Avoid Wrapping Executor Images ")," :"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Decouple the lifecycle of the executor and the boot process as seperate containers/images.")),(0,i.kt)("img",{src:"images/simplify_plugins_executor.png",alt:"Simplify Plugins Executor",width:"800"}),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"Task Boot Sequence"),":"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"Airflow worker fetches env and secrets for the job and adds them to the executor pod as environment variables."),(0,i.kt)("li",{parentName:"ol"},"KubernetesPodOperator spawns init-container and executor-container, mounted with shared volume (type emptyDir) for assets."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("inlineCode",{parentName:"li"},"init-container")," fetches assets, config, env files and writes onto the shared volume."),(0,i.kt)("li",{parentName:"ol"},"the default entrypoint in the executor-image starts the actual job.")),(0,i.kt)("h3",{id:"2-simplify-plugin-installations-"},"2. ",(0,i.kt)("u",null,"Simplify Plugin Installations")," :"),(0,i.kt)("h4",{id:"a-plugin-manager"},"A) Plugin Manager:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Currently the plugins are maintained as monorepo and versioned together. For any change in a single plugin, a new tar file containing all plugin binaries is created and released."),(0,i.kt)("li",{parentName:"ul"},"A plugin manager is required to support declarative installation of plugins so that plugins can be independently versioned, packaged and installed."),(0,i.kt)("li",{parentName:"ul"},"This plugin manager consumes a config (plugins_config) and downloads artifacts from a plugin repository.")),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Optimus support for plugin manager as below.",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"optimus plugin install -c config.yaml")," -- at server"))),(0,i.kt)("li",{parentName:"ul"},"Support for different kinds of plugin repositories (like s3, gcs, url, local file system etc..) gives the added flexibility and options to distribute and install the plugin binaries in different ways."),(0,i.kt)("li",{parentName:"ul"},"Plugins are installed at container runtime and this decouples the building of optimus docker image from plugins installations."),(0,i.kt)("li",{parentName:"ul"},"Example for the plugin_config: ")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"plugin:\n  dir: .plugins\n  artifacts:\n    # local filesystem for dev\n    - ../transformers/dist/bq2bq_darwin_arm64/optimus-bq2bq_darwin_arm64\n    # any http uri\n    - https://github.com/goto/optimus/releases/download/v0.2.5/optimus_0.2.5_linux_arm64.tar.gz\n      \n")),(0,i.kt)("h4",{id:"b-yaml-plugin-interface-for-client-side-simplification"},"B) Yaml Plugin Interface: (for client side simplification)"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Currently plugins are implemented and distributed as binaries and as clients needs to install them, it demands support for different host  architectures."),(0,i.kt)("li",{parentName:"ul"},"Since CLI (client side) plugins just require details about plugin such as Version, Suevery Questions etc. the proposal here is to maintain CLI plugins as yaml files."),(0,i.kt)("li",{parentName:"ul"},"Implementation wise, the proposal here is to split the current plugin interface (which only supports interaction with binary plugins) to also accommodate yaml based plugins."),(0,i.kt)("li",{parentName:"ul"},"The above mentioned pluign manager, at server end, would be agnostic about the contents of plugin artifacts from the repository."),(0,i.kt)("li",{parentName:"ul"},"At client side, the CLI could sync the yaml files from the server to stay up-to-date with the server w.r.t plugins."),(0,i.kt)("li",{parentName:"ul"},"At this point, we have the scope to move away from binary plugins except for bq2bq plugin due to its depdendency on ",(0,i.kt)("inlineCode",{parentName:"li"},"ComplileAsset")," and ",(0,i.kt)("inlineCode",{parentName:"li"},"ResolveDependency")," functionalities which are required at server end (not at cli)."),(0,i.kt)("li",{parentName:"ul"},"Handling Bq2Bq plugin:",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Move ",(0,i.kt)("inlineCode",{parentName:"li"},"CompileAsset")," functionality as a part of Bq2bq executor."),(0,i.kt)("li",{parentName:"ul"},"Move  ",(0,i.kt)("inlineCode",{parentName:"li"},"ResolveDependency")," functionality to optimus core which should support dependecy-resolution on standard-sql"))),(0,i.kt)("li",{parentName:"ul"},"Meanwhile the Bq2bq plugin is handled, the plugin interface can be maintanined in such a way that it also supports binary plugin in addition to yaml (as backward compaitbility feature)."),(0,i.kt)("li",{parentName:"ul"},"The plugin discovery logic should be to load binary if present, else load yaml file; for a single plugin."),(0,i.kt)("li",{parentName:"ul"},"Now that we have yaml files at server, CLI can sync only the yaml files from the server.",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"optimus plugin sync -c optimus.yaml"))))),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Example representation of the yaml plugin : ")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},'name: bq2bq\ndescription: BigQuery to BigQuery transformation task\nplugintype: task\npluginmods:\n  - cli\n  - dependencyresolver\npluginversion: 0.1.0-SNAPSHOT-27cb56f\napiversion: []\nimage: docker.io/goto/optimus-task-bq2bq-executor:0.1.0-SNAPSHOT-27cb56f\nsecretpath: /tmp/auth.json\ndependson: []\nhooktype: ""\n\nquestions:\n  - name: PROJECT\n    prompt: Project ID\n    help: Destination bigquery project ID\n    regexp: ^[a-zA-Z0-9_\\-]+$\n    validationerror: invalid name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)\n    minlength: 3\n  - name: Dataset\n    prompt: Dataset Name\n    help: Destination bigquery dataset ID\n    regexp: ^[a-zA-Z0-9_\\-]+$\n    validationerror: invalid name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)\n    minlength: 3\n  - name: TABLE\n    prompt: Table ID\n    help: Destination bigquery table ID\n    regexp: ^[a-zA-Z0-9_-]+$\n    validationerror: invalid table name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)\n    minlength: 3\n    maxlength: 1024\n  - name: LOAD_METHOD\n    prompt: Load method to use on destination\n    help: |\n      APPEND        - Append to existing table\n      REPLACE       - Deletes existing partition and insert result of select query\n      MERGE         - DML statements, BQ scripts\n      REPLACE_MERGE - [Experimental] Advanced replace using merge query\n    default: APPEND\n    multiselect:\n      - APPEND\n      - REPLACE\n      - MERGE\n      - REPLACE_MERGE\n      - REPLACE_ALL\ndefaultassets:\n  - name: query.sql\n    value: |\n      -- SQL query goes here\n\n      Select * from "project.dataset.table";\n      \n')),(0,i.kt)("h2",{id:"result"},"Result:"),(0,i.kt)("img",{src:"images/simplify_plugins.png",alt:"Simplify Plugins",width:"800"}),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Executor boot process is standardised and extracted away from plugin developers. Now any arbitrary image can be used for executors."),(0,i.kt)("li",{parentName:"ul"},"At server side, for changes in plugin (dur to plugin release), update the plugin_manager_config and restart the optimus server pod. The plugin manager is expected to reinstall the plugins."),(0,i.kt)("li",{parentName:"ul"},"Client side dependency on plugins is simplified with yaml based plugins.")))}c.isMDXComponent=!0}}]);